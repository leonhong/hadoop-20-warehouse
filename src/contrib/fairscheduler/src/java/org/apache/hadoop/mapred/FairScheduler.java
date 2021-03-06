/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobInProgress.DataStatistics;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class FairScheduler extends TaskScheduler {
  /** How often fair shares are re-calculated */
  public static long updateInterval = 500;
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FairScheduler");

  // Maximum locality delay when auto-computing locality delays
  private static final long MAX_AUTOCOMPUTED_LOCALITY_DELAY = 15000;
  private long dumpStatusPeriod = 300000; // 5 minute
  private long lastDumpStatusTime= 0L;

  protected int mapPerHeartBeat = 1;
  protected int reducePerHeartBeat = 1;
  protected PoolManager poolMgr;
  protected LoadManager loadMgr;
  protected TaskSelector taskSelector;
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected Map<JobInProgress, JobInfo> infos = // per-job scheduling variables
    new HashMap<JobInProgress, JobInfo>();
  protected JobInfoSummary infosummary = new JobInfoSummary();
  protected LinkedList<JobInProgress> sortedJobsByMapNeed, sortedJobsByReduceNeed;
  protected Comparator<JobInProgress> mapComparator, reduceComparator;
  
  protected long lastUpdateTime;           // Time when we last updated infos
  protected boolean initialized;  // Are we initialized?
  protected volatile boolean running; // Are we running?
  protected JobComparator jobComparator; // How to sort the jobs
  protected boolean assignMultiple; // Simultaneously assign map and reduce?
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected boolean waitForMapsBeforeLaunchingReduces = true;
  private Clock clock;
  private boolean runBackgroundUpdates; // Can be set to false for testing
  private JobListener jobListener;
  private JobInitializer jobInitializer;
  protected long lastHeartbeatTime;  // Time we last ran assignTasks
  protected long localityDelayNodeLocal; // Time to wait for node locality
  protected long localityDelayRackLocal;  // Time to wait for rack locality
  protected boolean autoComputeLocalityDelay = false; // Compute locality delay
                                                      // from heartbeat interval
  private Thread updateThread;

  protected LocalityLevelManager localManager = null;
  // a class which converts and obtains locality level

  // How often tasks are preempted (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long preemptionInterval = 15000;
  protected boolean preemptionEnabled;
  private long lastPreemptCheckTime; // Time we last ran preemptTasksIfNecessary
                             // Used for unit tests; disables background updates
  // Used to iterate through map and reduce task types
  private static final TaskType[] MAP_AND_REDUCE =
    new TaskType[] {TaskType.MAP, TaskType.REDUCE};

  private FairSchedulerMetricsInst fairSchedulerMetrics = null;

  
  /**
   * Class holding summary computations over all JobInfo objects
   */
  static class JobInfoSummary {
    int totalRunningMaps = 0; // sum over all infos.runningMaps
    int totalRunningReduces = 0; // sum over all infos.runningReduces
    int totalNeededMaps = 0; // sum over all infos.neededMaps
    int totalNeededReduces = 0; // sum over all infos.neededReduces

    public void reset () {
      totalRunningMaps = 0;
      totalRunningReduces = 0;
      totalNeededMaps = 0;
      totalNeededReduces = 0;
    }
  }

  /**
   * A class for holding per-job scheduler variables. These always contain the
   * values of the variables at the last update(), and are used along with a
   * time delta to update the map and reduce deficits before a new update().
   */
  static class JobInfo {
    boolean runnable = false;   // Can the job run given user/pool limits?
    // Does this job need to be initialized?
    volatile boolean needsInitializing = true;
    String poolName = "";       // The pool this job belongs to
    double mapWeight = 0;       // Weight of job in calculation of map share
    double reduceWeight = 0;    // Weight of job in calculation of reduce share
    long mapDeficit = 0;        // Time deficit for maps
    long reduceDeficit = 0;     // Time deficit for reduces
    int totalInitedTasks = 0;   // Total initialized tasks
    int runningMaps = 0;        // Maps running at last update
    int runningReduces = 0;     // Reduces running at last update
    int neededMaps;             // Maps needed at last update
    int neededReduces;          // Reduces needed at last update
    int minMaps = 0;            // Minimum maps as guaranteed by pool
    int minReduces = 0;         // Minimum reduces as guaranteed by pool
    double mapFairShare = 0;    // Fair share of map slots at last update
    double reduceFairShare = 0; // Fair share of reduce slots at last update
    int neededSpeculativeMaps;    // Speculative maps needed at last update
    int neededSpeculativeReduces; // Speculative reduces needed at last update
    // Variables used for delay scheduling
    LocalityLevel lastMapLocalityLevel = LocalityLevel.NODE;
    // Locality level of last map launched
    long timeWaitedForLocalMap; // Time waiting for local map since last map
    boolean skippedAtLastHeartbeat;  // Was job skipped at previous assignTasks?
                                     // (used to update timeWaitedForLocalMap)
     // Variables used for preemption
     long lastTimeAtMapMinShare;      // When was the job last at its min maps?
     long lastTimeAtReduceMinShare;   // Similar for reduces.
     long lastTimeAtMapHalfFairShare; // When was the job last at half fair maps?
     long lastTimeAtReduceHalfFairShare;  // Similar for reduces.

     public JobInfo(long currentTime) {
       lastTimeAtMapMinShare = currentTime;
       lastTimeAtReduceMinShare = currentTime;
       lastTimeAtMapHalfFairShare = currentTime;
       lastTimeAtReduceHalfFairShare = currentTime;
     }
  }

  /**
   *  A class which converts and obtains locality level
   */
  static class LocalityLevelManager {
    /**
     * Obtain LocalityLevel of a task from its job and tasktracker.
     */
    public LocalityLevel taskToLocalityLevel(JobInProgress job,
        Task mapTask, TaskTrackerStatus tracker) {
      TaskInProgress tip = getTaskInProgress(job, mapTask);
      switch (job.getLocalityLevel(tip, tracker)) {
      case 0: return LocalityLevel.NODE;
      case 1: return LocalityLevel.RACK;
      default: return LocalityLevel.ANY;
      }
    }

    private TaskInProgress getTaskInProgress(JobInProgress job, Task mapTask) {
      if (!job.inited()) {
        return null;
      }
      TaskID tipId = mapTask.getTaskID().getTaskID();
      for (int i = 0; i < job.maps.length; i++) {
        if (tipId.equals(job.maps[i].getTIPId())) {
          return job.maps[i];
        }
      }
      return null;
    }
  }

  /**
   * Represents the level of data-locality at which a job in the fair scheduler
   * is allowed to launch tasks. By default, jobs are not allowed to launch
   * non-data-local tasks until they have waited a small number of seconds to
   * find a slot on a node that they have data on. If a job has waited this
   * long, it is allowed to launch rack-local tasks as well (on nodes that may
   * not have the task's input data, but share a rack with a node that does).
   * Finally, after a further wait, jobs are allowed to launch tasks anywhere
   * in the cluster.
   */
  public enum LocalityLevel {
    NODE (1),
    RACK (2),
    ANY  (Integer.MAX_VALUE);
    private final int cacheLevelCap;
    LocalityLevel(int cacheLevelCap) {
      this.cacheLevelCap = cacheLevelCap;
    }
    /**
     * Obtain a JobInProgress cache level cap to pass to
     * {@link JobInProgress#obtainNewMapTask(TaskTrackerStatus, int, int, int)}
     * to ensure that only tasks of this locality level and lower are launched.
     */
    public int getCacheLevelCap() {
      return cacheLevelCap;
    }
  }

  /**
   * A clock class - can be mocked out for testing.
   */
  static class Clock {
    long getTime() {
      return System.currentTimeMillis();
    }
  }

  public FairScheduler() {
    this(new Clock(), true, new LocalityLevelManager());
  }

  /**
   * Constructor used for tests, which can change the clock, disable updates
   * and change locality.
   */
  protected FairScheduler(Clock clock, boolean runBackgroundUpdates,
                          LocalityLevelManager localManager) {
    this.clock = clock;
    this.runBackgroundUpdates = runBackgroundUpdates;
    this.jobListener = new JobListener();

    this.localManager = localManager;
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();

      jobInitializer = new JobInitializer(conf, taskTrackerManager);

      taskTrackerManager.addJobInProgressListener(jobListener);
      poolMgr = new PoolManager(conf);
      loadMgr = (LoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.loadmanager",
              CapBasedLoadManager.class, LoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
      loadMgr.start();
      taskSelector = (TaskSelector) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.taskselector",
              DefaultTaskSelector.class, TaskSelector.class), conf);
      taskSelector.setTaskTrackerManager(taskTrackerManager);
      taskSelector.start();
      Class<?> weightAdjClass = conf.getClass(
          "mapred.fairscheduler.weightadjuster", null);
      if (weightAdjClass != null) {
        weightAdjuster = (WeightAdjuster) ReflectionUtils.newInstance(
            weightAdjClass, conf);
      }
      updateInterval = conf.getLong(
          "mapred.fairscheduler.update.interval", updateInterval);
      preemptionInterval = conf.getLong(
          "mapred.fairscheduler.preemption.interval", preemptionInterval);
      assignMultiple = conf.getBoolean(
          "mapred.fairscheduler.assignmultiple", false);
      sizeBasedWeight = conf.getBoolean(
          "mapred.fairscheduler.sizebasedweight", false);
      preemptionEnabled = conf.getBoolean(
          "mapred.fairscheduler.preemption", false);

      mapPerHeartBeat =
        conf.getInt("mapred.fairscheduler.mapsperheartbeat", 1);
      reducePerHeartBeat =
        conf.getInt("mapred.fairscheduler.reducesperheartbeat", 1);
      jobComparator = JobComparator.fromString(
          conf.get("mapred.fairscheduler.jobcomparator",
                   JobComparator.DEFICIT.toString()));
      long defaultDelay = conf.getLong(
          "mapred.fairscheduler.locality.delay", -1);
      localityDelayNodeLocal = conf.getLong(
          "mapred.fairscheduler.locality.delay.nodelocal", defaultDelay);
      localityDelayRackLocal = conf.getLong(
          "mapred.fairscheduler.locality.delay.racklocal", defaultDelay);
      dumpStatusPeriod = conf.getLong(
          "mapred.fairscheduler.dump.status.period", dumpStatusPeriod);
      if (defaultDelay == -1 &&
          (localityDelayNodeLocal == -1 || localityDelayRackLocal == -1)) {
         autoComputeLocalityDelay = true; // Compute from heartbeat interval
      }

      initialized = true;
      running = true;
      lastUpdateTime = clock.getTime();
      // Start a thread to update deficits every updateInterval
      if (runBackgroundUpdates) {
        updateThread = new UpdateThread();
        updateThread.start();
      }
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        HttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            FairSchedulerServlet.class);
        fairSchedulerMetrics = new FairSchedulerMetricsInst(this, conf);
      }

    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start FairScheduler", e);
    }
    LOG.info("Successfully configured FairScheduler");
  }

  @Override
  public void terminate() throws IOException {
    running = false;
    jobInitializer.terminate();
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
  }

  private class JobInitializer {
    private final int DEFAULT_NUM_THREADS = 1;
    private ExecutorService threadPool;
    private TaskTrackerManager ttm;


    public JobInitializer(Configuration conf, TaskTrackerManager ttm) {
      int numThreads = conf.getInt("mapred.jobinit.threads",
                                   DEFAULT_NUM_THREADS);
      threadPool = Executors.newFixedThreadPool(numThreads);
      this.ttm = ttm;
    }

    public void initJob(JobInfo jobInfo, JobInProgress job) {
      if (runBackgroundUpdates) {
        threadPool.execute(new InitJob(jobInfo, job));
      } else {
        new InitJob(jobInfo, job).run();
      }
    }

    class InitJob implements Runnable {
      private JobInfo jobInfo;
      private JobInProgress job;

      public InitJob(JobInfo jobInfo, JobInProgress job) {
        this.jobInfo = jobInfo;
        this.job = job;
      }

      public void run() {
        ttm.initJob(job);
      }
    }

    void terminate() {
      LOG.info("Shutting down thread pool");
      threadPool.shutdownNow();
      try {
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        // Ignore, we are in shutdown anyway.
      }
    }
  }

  /**
   * Used to listen for jobs added/removed by our {@link TaskTrackerManager}.
   */
  private class JobListener extends JobInProgressListener {

    @Override
    public void jobAdded(JobInProgress job) {
      synchronized (FairScheduler.this) {
        poolMgr.addJob(job);
        JobInfo info = new JobInfo(clock.getTime());
        info.poolName = poolMgr.getPoolName(job);
        infos.put(job, info);
        if (updateThread != null)
          updateThread.interrupt();
        else
          update();
      }
    }

    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (FairScheduler.this) {
        poolMgr.removeJob(job);
        infos.remove(job);
        if(sortedJobsByMapNeed != null) 
          sortedJobsByMapNeed.remove(job);
        if(sortedJobsByReduceNeed != null) 
          sortedJobsByReduceNeed.remove(job);
      }
    }

    @Override
    public void jobUpdated(JobChangeEvent event) {
    }
  }

  /**
   * A thread which calls {@link FairScheduler#update()} ever
   * <code>updateInterval</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("FairScheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          try {
            Thread.sleep(updateInterval);
          } catch (InterruptedException e) {
            // ignore
          }
          update();
          preemptTasksIfNecessary();
        } catch (Exception e) {
          LOG.error("Exception in fair scheduler UpdateThread", e);
        }
      }
    }
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTracker tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;

    int totalRunnableMaps = infosummary.totalRunningMaps + 
      infosummary.totalNeededMaps;
    int totalRunnableReduces = infosummary.totalRunningReduces + 
      infosummary.totalNeededReduces;

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Compute total map/reduce slots
    // In the future we can precompute this if the Scheduler becomes a
    // listener of tracker join/leave events.
    int totalMapSlots = getTotalSlots(TaskType.MAP, clusterStatus);
    int totalReduceSlots = getTotalSlots(TaskType.REDUCE, clusterStatus);

    // Scan to see whether any job needs to run a map, then a reduce
    ArrayList<Task> tasks = new ArrayList<Task>();
    long currentTime = clock.getTime();
    // Update time waited for local maps for jobs skipped on last heartbeat
    updateLocalityWaitTimes(currentTime);
    TaskTrackerStatus trackerStatus = tracker.getStatus();
    int availableMapsOnTT = trackerStatus.getAvailableMapSlots();
    int availableReducesOnTT = trackerStatus.getAvailableReduceSlots();
    for (TaskType taskType: MAP_AND_REDUCE) {
      boolean canAssign = (taskType == TaskType.MAP) ?
          loadMgr.canAssignMap(trackerStatus, totalRunnableMaps,
                               totalMapSlots) :
          loadMgr.canAssignReduce(trackerStatus, totalRunnableReduces,
                                  totalReduceSlots);
      boolean hasAvailableSlots =
        (availableMapsOnTT > 0 && taskType == TaskType.MAP) ||
        (availableReducesOnTT > 0 && taskType == TaskType.REDUCE);
      if (!canAssign || !hasAvailableSlots) {
        continue; // Go to the next task type
      }

      int numTasks = 0;
      LinkedList<JobInProgress> candidates = (taskType == TaskType.MAP) ?
        sortedJobsByMapNeed : sortedJobsByReduceNeed;
      if (candidates == null) {
        // There are no candidate jobs
        // Only happens when the cluster is empty
        break;
      }
      LinkedList<JobInProgress> jobsToReinsert = new LinkedList<JobInProgress> ();
      Iterator<JobInProgress> iterator = candidates.iterator();

      while (iterator.hasNext()) {
        JobInProgress job = iterator.next();

        if (job.getStatus().getRunState() != JobStatus.RUNNING) {
          iterator.remove();
          continue;
        }

        if (!loadMgr.canLaunchTask(trackerStatus, job, taskType)) {
          continue;
        }
        // Do not schedule if the maximum slots is reached in the pool.
        JobInfo info = infos.get(job);
        if (poolMgr.isMaxTasks(info.poolName, taskType)) {
          continue;
        }
        // Try obtaining a suitable task for this job
        Task task = null;
        if (taskType == TaskType.MAP) {
          LocalityLevel level = getAllowedLocalityLevel(job, currentTime);
          task = job.obtainNewMapTask(trackerStatus,
                         clusterStatus.getTaskTrackers(),
                         taskTrackerManager.getNumberOfUniqueHosts(),
                         level.getCacheLevelCap());
          if (task == null) {
            info.skippedAtLastHeartbeat = true;
          } else {
            updateLastMapLocalityLevel(job, task, trackerStatus);
          }
        } else {
          task = job.obtainNewReduceTask(trackerStatus,
                         clusterStatus.getTaskTrackers(),
                         taskTrackerManager.getNumberOfUniqueHosts());
        }
        // Update information when obtained a task
        if (task != null) {
          // Update the JobInfo for this job so we account for the launched
          // tasks during this update interval and don't try to launch more
          // tasks than the job needed on future heartbeats
          if (taskType == TaskType.MAP) {
            info.runningMaps++;
            info.neededMaps--;
            infosummary.totalRunningMaps++;
            infosummary.totalNeededMaps--;
          } else {
            info.runningReduces++;
            info.neededReduces--;
            infosummary.totalRunningReduces++;
            infosummary.totalNeededReduces--;
          }
          poolMgr.incRunningTasks(info.poolName, taskType, 1);
          tasks.add(task);
          numTasks++;

          // delete the scheduled jobs from sorted list
          iterator.remove();  

          // keep track that it needs to be reinserted.
          // we reinsert in LIFO order to minimize comparisons
          if (neededTasks(job, taskType) > 0)
            jobsToReinsert.push(job);

          if (!assignMultiple) {
            if (jobsToReinsert.size() > 0)
              mergeJobs(jobsToReinsert, taskType);
            return tasks;
          }

          if (numTasks >= ((taskType == TaskType.MAP)
                          ? mapPerHeartBeat : reducePerHeartBeat)) {
            break;
          }
          if (numTasks >= ((taskType == TaskType.MAP)
                          ? availableMapsOnTT : availableReducesOnTT)) {
            break;
          }
        }
      }

      if (jobsToReinsert.size() > 0)
        mergeJobs(jobsToReinsert, taskType);
    }

    // If no tasks were found, return null
    return tasks.isEmpty() ? null : tasks;
  }

  /**
   * reinsert a set of jobs into the sorted jobs for a given type (MAP/REDUCE)
   * the re-insertion happens in place.
   * we are exploiting the property that the jobs being inserted will most likely end
   * up at the head of the sorted list and not require a lot comparisons
   */
  private void mergeJobs (LinkedList<JobInProgress> jobsToReinsert, TaskType taskType) {
    LinkedList<JobInProgress> sortedJobs = (taskType == TaskType.MAP) ?
      sortedJobsByMapNeed : sortedJobsByReduceNeed;
    Comparator<JobInProgress> comparator = (taskType == TaskType.MAP) ?
      mapComparator :  reduceComparator;

    // for each job to be reinserted
    for(JobInProgress jobToReinsert: jobsToReinsert) {

      // look at existing jobs in the sorted list starting with the head
      boolean reinserted = false;
      ListIterator<JobInProgress> iter = sortedJobs.listIterator(0);
      while (iter.hasNext()) {
        JobInProgress job = iter.next();
        if (comparator.compare(jobToReinsert, job) < 0) {
          // found the point of insertion, move the iterator back one step
          iter.previous();
          // now we are positioned before the job we compared against
          // insert it before this job
          iter.add(jobToReinsert);
          reinserted = true;
          break;
        }
      }
      if (!reinserted) {
        sortedJobs.add(jobToReinsert);
      }
    }
  }


  /**
   * Compare jobs by deficit for a given task type, putting jobs whose current
   * allocation is less than their minimum share always ahead of others. This is
   * the default job comparator used for Fair Sharing.
   */
  private class DeficitComparator implements Comparator<JobInProgress> {
    private final TaskType taskType;

    private DeficitComparator(TaskType taskType) {
      this.taskType = taskType;
    }

    public int compare(JobInProgress j1, JobInProgress j2) {
      // Put needy jobs ahead of non-needy jobs (where needy means must receive
      // new tasks to meet slot minimum), comparing among jobs of the same type
      // by deficit so as to put jobs with higher deficit ahead.
      JobInfo j1Info = infos.get(j1);
      JobInfo j2Info = infos.get(j2);
      double deficitDif;
      boolean job1BelowMinSlots, job2BelowMinSlots;

      if (taskType == TaskType.MAP) {
        job1BelowMinSlots = j1.runningMaps() < j1Info.minMaps;
        job2BelowMinSlots = j2.runningMaps() < j2Info.minMaps;
        deficitDif = j2Info.mapDeficit - j1Info.mapDeficit;
      } else {
        job1BelowMinSlots = j1.runningReduces() < j1Info.minReduces;
        job2BelowMinSlots = j2.runningReduces() < j2Info.minReduces;
        deficitDif = j2Info.reduceDeficit - j1Info.reduceDeficit;
      }
      // Compute if the pool minimum slots limit has been achieved
      String pool1 = j1Info.poolName;
      String pool2 = j2Info.poolName;
      boolean pool1BelowMinSlots = poolMgr.getRunningTasks(pool1, taskType) <
                                   poolMgr.getAllocation(pool1, taskType);
      boolean pool2BelowMinSlots = poolMgr.getRunningTasks(pool2, taskType) <
                                   poolMgr.getAllocation(pool2, taskType);

      // A job is needy only when both of the job and pool minimum slots are
      // not reached.
      boolean job1Needy = pool1BelowMinSlots && job1BelowMinSlots;
      boolean job2Needy = pool2BelowMinSlots && job2BelowMinSlots;

      if (job1Needy && !job2Needy) {
        return -1;
      } else if (job2Needy && !job1Needy) {
        return 1;
      } else {  // Both needy or both non-needy; compare by deficit
        return (int) Math.signum(deficitDif);
      }
    }
  }

  /**
   * Compare jobs by current running tasks for a given task type. We first
   * compare if jobs are running under minimum slots. Job with tasks under
   * minimum slots will be ranked higher. And we compare the ratio of running
   * tasks and the fairshare to rank the job.
   */
  private class FairComparator implements Comparator<JobInProgress> {
    private final TaskType taskType;

    private FairComparator(TaskType taskType) {
      this.taskType = taskType;
    }

    public int compare(JobInProgress j1, JobInProgress j2) {
      JobInfo j1Info = infos.get(j1);
      JobInfo j2Info = infos.get(j2);
      int job1RunningTasks, job2RunningTasks;
      int job1MinTasks, job2MinTasks;
      double job1Weight, job2Weight;
      // Get running tasks, minimum tasks and weight based on task type.
      if (taskType == TaskType.MAP) {
        job1RunningTasks = j1Info.runningMaps;
        job1MinTasks = j1Info.minMaps;
        job1Weight = j1Info.mapWeight;
        job2RunningTasks = j2Info.runningMaps;
        job2MinTasks = j2Info.minMaps;
        job2Weight = j2Info.mapWeight;
      } else {
        job1RunningTasks = j1Info.runningReduces;
        job1MinTasks = j1Info.minReduces;
        job1Weight = j1Info.reduceWeight;
        job2RunningTasks = j2Info.runningReduces;
        job2MinTasks = j2Info.minReduces;
        job2Weight = j2Info.reduceWeight;
      }

      // Compute the ratio between running tasks and fairshare (or minslots)
      boolean job1BelowMinSlots, job2BelowMinSlots;
      double job1RunningTaskRatio, job2RunningTaskRatio;
      if (job1RunningTasks < job1MinTasks) {
        job1BelowMinSlots = true;
        job1RunningTaskRatio = (double)job1RunningTasks /
                               (double)job1MinTasks;
      } else {
        job1BelowMinSlots = false;
        job1RunningTaskRatio = (double)job1RunningTasks /
                               job1Weight;
      }
      if (job2RunningTasks < job2MinTasks) {
        job2BelowMinSlots = true;
        job2RunningTaskRatio = (double)job2RunningTasks /
                               (double)job2MinTasks;
      } else {
        job2BelowMinSlots = false;
        job2RunningTaskRatio = (double)job2RunningTasks /
                               job2Weight;
      }

      // Compute if the pool minimum slots limit has been achieved
      String pool1 = j1Info.poolName;
      String pool2 = j2Info.poolName;
      boolean pool1BelowMinSlots = poolMgr.getRunningTasks(pool1, taskType) <
                                   poolMgr.getAllocation(pool1, taskType);
      boolean pool2BelowMinSlots = poolMgr.getRunningTasks(pool2, taskType) <
                                   poolMgr.getAllocation(pool2, taskType);

      // A job is needy only when both of the job and pool minimum slots are
      // not reached.
      boolean job1Needy = pool1BelowMinSlots && job1BelowMinSlots;
      boolean job2Needy = pool2BelowMinSlots && job2BelowMinSlots;
      if (job1Needy && !job2Needy) {
        return -1;
      } else if (job2Needy && !job1Needy) {
        return 1;
      } else if (job1RunningTaskRatio == job2RunningTaskRatio) {
        return j1.getJobID().toString().compareTo(j2.getJobID().toString());
      } else {  // Both needy or both non-needy; compare by running task ratio
        return job1RunningTaskRatio <= job2RunningTaskRatio ? -1 : 1;
      }
    }
  }

  /**
   * Update locality wait times for jobs that were skipped at last heartbeat.
   */
  private void updateLocalityWaitTimes(long currentTime) {
    long timeSinceLastHeartbeat =
      (lastHeartbeatTime == 0 ? 0 : currentTime - lastHeartbeatTime);
    lastHeartbeatTime = currentTime;
    for (JobInfo info: infos.values()) {
      if (info.skippedAtLastHeartbeat) {
        info.timeWaitedForLocalMap += timeSinceLastHeartbeat;
        // We reset the flag so that timeWaitedForLocalMap is increment only
        // once. It will be increment again if skippedAtLastHeartbeat is set
        // to true next time.
        info.skippedAtLastHeartbeat = false;
      }
    }
  }

  /**
   * Update a job's locality level and locality wait variables given that that
   * it has just launched a map task on a given task tracker.
   */
  private void updateLastMapLocalityLevel(JobInProgress job,
      Task mapTaskLaunched, TaskTrackerStatus tracker) {
    JobInfo info = infos.get(job);
    LocalityLevel localityLevel = localManager.taskToLocalityLevel(
        job, mapTaskLaunched, tracker);
    info.lastMapLocalityLevel = localityLevel;
    info.timeWaitedForLocalMap = 0;
  }

  /**
   * Get the maximum locality level at which a given job is allowed to
   * launch tasks, based on how long it has been waiting for local tasks.
   * This is used to implement the "delay scheduling" feature of the Fair
   * Scheduler for optimizing data locality.
   * If the job has no locality information (e.g. it does not use HDFS), this
   * method returns LocalityLevel.ANY, allowing tasks at any level.
   * Otherwise, the job can only launch tasks at its current locality level
   * or lower, unless it has waited at least localityDelayNodeLocal or
   * localityDelayRackLocal milliseconds depends on the current level. If it
   * has waited (localityDelayNodeLocal + localityDelayRackLocal) milliseconds,
   * it can go to any level.
   */
  protected LocalityLevel getAllowedLocalityLevel(JobInProgress job,
      long currentTime) {
    JobInfo info = infos.get(job);
    if (info == null) { // Job not in infos (shouldn't happen)
      LOG.error("getAllowedLocalityLevel called on job " + job
          + ", which does not have a JobInfo in infos");
      return LocalityLevel.ANY;
    }
    if (job.nonLocalMaps.size() > 0) { // Job doesn't have locality information
      return LocalityLevel.ANY;
    }
    // In the common case, compute locality level based on time waited
    switch(info.lastMapLocalityLevel) {
    case NODE: // Last task launched was node-local
      if (info.timeWaitedForLocalMap >=
          (localityDelayNodeLocal + localityDelayRackLocal))
        return LocalityLevel.ANY;
      else if (info.timeWaitedForLocalMap >= localityDelayNodeLocal)
        return LocalityLevel.RACK;
      else
        return LocalityLevel.NODE;
    case RACK: // Last task launched was rack-local
      if (info.timeWaitedForLocalMap >= localityDelayRackLocal)
        return LocalityLevel.ANY;
      else
        return LocalityLevel.RACK;
    default: // Last task was non-local; can launch anywhere
      return LocalityLevel.ANY;
    }
  }

  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and numbers of running
   * and needed tasks of each type.
   */
  protected void update() {
    //Making more granual locking so that clusterStatus can be fetched from Jobtracker.
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Recompute locality delay from JobTracker heartbeat interval if enabled.
    // This will also lock the JT, so do it outside of a fair scheduler lock.
    if (autoComputeLocalityDelay) {
      JobTracker jobTracker = (JobTracker) taskTrackerManager;
      localityDelayNodeLocal = Math.min(MAX_AUTOCOMPUTED_LOCALITY_DELAY,
           (long) (1.5 * jobTracker.getNextHeartbeatInterval()));
      localityDelayRackLocal = localityDelayNodeLocal;
    }
    // Got clusterStatus hence acquiring scheduler lock now
    // Remove non-running jobs
    synchronized(this){

      // Reload allocations file if it hasn't been loaded in a while
      poolMgr.reloadAllocsIfNecessary();

      List<JobInProgress> toRemove = new ArrayList<JobInProgress>();
      for (JobInProgress job: infos.keySet()) {
        int runState = job.getStatus().getRunState();
        if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED
          || runState == JobStatus.KILLED) {
            toRemove.add(job);
        }
      }
      for (JobInProgress job: toRemove) {
        infos.remove(job);
        poolMgr.removeJob(job);
      }
      // Update running jobs with deficits since last update, and compute new
      // slot allocations, weight, shares and task counts
      long now = clock.getTime();
      long timeDelta = now - lastUpdateTime;
      updateDeficits(timeDelta);
      updateRunnability();
      updateTaskCounts();
      updateWeights();
      updateMinSlots();
      updateFairShares(clusterStatus);
      if (preemptionEnabled) {
        updatePreemptionVariables();
      }
      sortJobs();
      dumpStatus(now);
      lastUpdateTime = now;
    }
  }

  /**
   * Output some scheduling information to LOG
   * @param now current unix time
   */
  private void dumpStatus(long now) {
    if (now - lastDumpStatusTime < dumpStatusPeriod) {
      return;
    }
    lastDumpStatusTime = now;
    dumpSpeculationStatus(now);
  }

  private void dumpSpeculationStatus(long now) {
    final long TASK_INFO_DUMP_DELAY = 1200000; // 20 minutes
    for (JobInProgress job : infos.keySet()) {
      for (TaskType type : MAP_AND_REDUCE) {
        boolean is_map = (type == TaskType.MAP);
        if (!is_map && job.desiredReduces() <= 0)
          continue;

        DataStatistics taskStats =
            job.getRunningTaskStatistics(is_map);
        LOG.info(job.getJobID().toString() + " taskStats : " + taskStats);

        for (TaskInProgress tip :
               job.getTasks(is_map ? org.apache.hadoop.mapreduce.TaskType.MAP :
                            org.apache.hadoop.mapreduce.TaskType.REDUCE)) {
          if (!tip.isComplete() &&
              now - tip.getLastDispatchTime() > TASK_INFO_DUMP_DELAY) {
            double currProgRate = tip.getCurrentProgressRate(now);
            TreeMap<TaskAttemptID, String> activeTasks = tip.getActiveTasks();
            if (activeTasks.isEmpty()) {
              continue;
            }
            boolean canBeSpeculated = tip.canBeSpeculated(now);
            LOG.info(activeTasks.firstKey() +
                "activeTasks.size(): " + activeTasks.size() + " " +
                " task's progressrate: " + currProgRate +
                " canBeSepculated : " + canBeSpeculated);
          }
        }
      }
    }
  }

  private void sortJobs() {
    for (TaskType taskType: MAP_AND_REDUCE) {
      // Sort jobs by deficit (for Fair Sharing), submit time (for FIFO) or
      // current running task ratio
      Comparator<JobInProgress> comparator;
      switch(jobComparator) {
      case FAIR: 
        comparator = new FairComparator(taskType);
        break;
      case FIFO:
        comparator = new FifoJobComparator();
        break;
      default:
        comparator = new DeficitComparator(taskType);
      }

      // Figure out the jobs that need this type of task
      LinkedList<JobInProgress> sortedJobs = new LinkedList<JobInProgress>();
      for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
        JobInProgress job = entry.getKey();
        JobInfo jobInfo = entry.getValue();
        if (job.getStatus().getRunState() == JobStatus.RUNNING &&
            neededTasks(jobInfo, taskType) > 0) {
          sortedJobs.add(job);
        }
      }
      Collections.sort (sortedJobs, comparator);

      if (taskType == TaskType.MAP)  {
        sortedJobsByMapNeed = sortedJobs;
        mapComparator = comparator;
      } else {
        sortedJobsByReduceNeed = sortedJobs;
        reduceComparator = comparator;
      }
    }
  }


  private void updateDeficits(long timeDelta) {
    for (JobInfo info: infos.values()) {
      info.mapDeficit +=
        (info.mapFairShare - info.runningMaps) * timeDelta;
      info.reduceDeficit +=
        (info.reduceFairShare - info.runningReduces) * timeDelta;
    }
  }

  private void updateRunnability() {
    // Start by marking everything as not runnable
    for (JobInfo info: infos.values()) {
      info.runnable = false;
    }
    // Create a list of sorted jobs in order of start time and priority
    List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
    Collections.sort(jobs, new FifoJobComparator());

    // Mark jobs as runnable in order of start time and priority, until
    // user or pool limits have been reached.
    Map<String, Integer> userJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolTasks = new HashMap<String, Integer>();
    Set<JobInProgress> couldBeInitialized = new HashSet<JobInProgress>();
    for (JobInProgress job: jobs) {
      String user = job.getJobConf().getUser();
      String pool = poolMgr.getPoolName(job);
      int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
      int poolCount = poolJobs.containsKey(pool) ? poolJobs.get(pool) : 0;
      int poolTaskCount = poolTasks.containsKey(pool) ? poolTasks.get(pool) : 0;
      if (userCount < poolMgr.getUserMaxJobs(user) &&
          poolCount < poolMgr.getPoolMaxJobs(pool) &&
          poolTaskCount < poolMgr.getPoolMaxInitedTasks(pool)) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING ||
            job.getStatus().getRunState() == JobStatus.PREP) {
          userJobs.put(user, userCount + 1);
          poolJobs.put(pool, poolCount + 1);
          poolTasks.put(pool, poolTaskCount + infos.get(job).totalInitedTasks);
          JobInfo jobInfo = infos.get(job);
          if (job.getStatus().getRunState() == JobStatus.RUNNING) {
            jobInfo.runnable = true;
          } else {
            // The job is in the PREP state. Give it to the job initializer
            // for initialization if we have not already done it.
            if (jobInfo.needsInitializing) {
              jobInfo.needsInitializing = false;
              jobInitializer.initJob(jobInfo, job);
            }
          }
        }
      }
    }
  }

  private void updateTaskCounts() {
    poolMgr.resetRunningTasks(TaskType.MAP);
    poolMgr.resetRunningTasks(TaskType.REDUCE);
    infosummary.reset();
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      if (job.getStatus().getRunState() != JobStatus.RUNNING)
        continue; // Job is still in PREP state and tasks aren't initialized
      // Count maps
      int totalMaps = job.numMapTasks;
      int finishedMaps = 0;
      int runningMaps = 0;
      for (TaskInProgress tip :
           job.getTasks(org.apache.hadoop.mapreduce.TaskType.MAP)) {
        if (tip.isComplete()) {
          finishedMaps += 1;
        } else if (tip.isRunning()) {
          runningMaps += tip.getActiveTasks().size();
        }
      }
      info.totalInitedTasks = job.numMapTasks + job.numReduceTasks;
      info.runningMaps = runningMaps;
      infosummary.totalRunningMaps += runningMaps;
      poolMgr.incRunningTasks(info.poolName, TaskType.MAP, runningMaps);
      info.neededSpeculativeMaps =  taskSelector.neededSpeculativeMaps(job);
      info.neededMaps = (totalMaps - runningMaps - finishedMaps
          + info.neededSpeculativeMaps);
      // Count reduces
      int totalReduces = job.numReduceTasks;
      int finishedReduces = 0;
      int runningReduces = 0;
      for (TaskInProgress tip :
           job.getTasks(org.apache.hadoop.mapreduce.TaskType.REDUCE)) {
        if (tip.isComplete()) {
          finishedReduces += 1;
        } else if (tip.isRunning()) {
          runningReduces += tip.getActiveTasks().size();
        }
      }
      info.runningReduces = runningReduces;
      infosummary.totalRunningReduces += runningReduces;
      poolMgr.incRunningTasks(info.poolName, TaskType.REDUCE, runningReduces);
      if (job.scheduleReduces()) {
        info.neededSpeculativeReduces =
          taskSelector.neededSpeculativeReduces(job);
        info.neededReduces = (totalReduces - runningReduces - finishedReduces
            + info.neededSpeculativeReduces);
      } else {
        info.neededReduces = 0;
      }
      // If the job was marked as not runnable due to its user or pool having
      // too many active jobs, set the neededMaps/neededReduces to 0. We still
      // count runningMaps/runningReduces however so we can give it a deficit.
      if (!info.runnable) {
        info.neededMaps = 0;
        info.neededReduces = 0;
      }
      infosummary.totalNeededMaps += info.neededMaps;
      infosummary.totalNeededReduces += info.neededReduces;
    }
  }

  private void updateWeights() {
    // First, calculate raw weights for each job
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      info.mapWeight = calculateRawWeight(job, TaskType.MAP);
      info.reduceWeight = calculateRawWeight(job, TaskType.REDUCE);
    }
    // Now calculate job weight sums for each pool
    Map<String, Double> mapWeightSums = new HashMap<String, Double>();
    Map<String, Double> reduceWeightSums = new HashMap<String, Double>();
    for (Pool pool: poolMgr.getPools()) {
      double mapWeightSum = 0;
      double reduceWeightSum = 0;
      for (JobInProgress job: pool.getJobs()) {
        if (isRunnable(job)) {
          if (runnableTasks(job, TaskType.MAP) > 0) {
            mapWeightSum += infos.get(job).mapWeight;
          }
          if (runnableTasks(job, TaskType.REDUCE) > 0) {
            reduceWeightSum += infos.get(job).reduceWeight;
          }
        }
      }
      mapWeightSums.put(pool.getName(), mapWeightSum);
      reduceWeightSums.put(pool.getName(), reduceWeightSum);
    }
    // And normalize the weights based on pool sums and pool weights
    // to share fairly across pools (proportional to their weights)
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      String pool = poolMgr.getPoolName(job);
      double poolWeight = poolMgr.getPoolWeight(pool);
      double mapWeightSum = mapWeightSums.get(pool);
      double reduceWeightSum = reduceWeightSums.get(pool);
      if (mapWeightSum == 0)
        info.mapWeight = 0;
      else
        info.mapWeight *= (poolWeight / mapWeightSum);
      if (reduceWeightSum == 0)
        info.reduceWeight = 0;
      else
        info.reduceWeight *= (poolWeight / reduceWeightSum);
    }
  }

  private void updateMinSlots() {
    // Clear old minSlots
    for (JobInfo info: infos.values()) {
      info.minMaps = 0;
      info.minReduces = 0;
    }
    // For each pool, distribute its task allocation among jobs in it that need
    // slots. This is a little tricky since some jobs in the pool might not be
    // able to use all the slots, e.g. they might have only a few tasks left.
    // To deal with this, we repeatedly split up the available task slots
    // between the jobs left, give each job min(its alloc, # of slots it needs),
    // and redistribute any slots that are left over between jobs that still
    // need slots on the next pass. If, in total, the jobs in our pool don't
    // need all its allocation, we leave the leftover slots for general use.
    PoolManager poolMgr = getPoolManager();
    for (Pool pool: poolMgr.getPools()) {
      for (final TaskType type: MAP_AND_REDUCE) {
        Set<JobInProgress> jobs = new HashSet<JobInProgress>(pool.getJobs());
        int slotsLeft = poolMgr.getAllocation(pool.getName(), type);
        // Keep assigning slots until none are left
        while (slotsLeft > 0) {
          // Figure out total weight of jobs that still need slots
          double totalWeight = 0;
          for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
            JobInProgress job = it.next();
            if (isRunnable(job) &&
                runnableTasks(job, type) > minTasks(job, type)) {
              totalWeight += weight(job, type);
            } else {
              it.remove();
            }
          }
          if (totalWeight == 0) // No jobs that can use more slots are left
            break;
          // Assign slots to jobs, using the floor of their weight divided by
          // total weight. This ensures that all jobs get some chance to take
          // a slot. Then, if no slots were assigned this way, we do another
          // pass where we use ceil, in case some slots were still left over.
          int oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
          for (JobInProgress job: jobs) {
            double weight = weight(job, type);
            int share = (int) Math.floor(oldSlots * weight / totalWeight);
            slotsLeft = giveMinSlots(job, type, slotsLeft, share);
          }
          if (slotsLeft == oldSlots) {
            // No tasks were assigned; do another pass using ceil, giving the
            // extra slots to jobs in order of weight then deficit
            List<JobInProgress> sortedJobs = new ArrayList<JobInProgress>(jobs);
            Collections.sort(sortedJobs, new Comparator<JobInProgress>() {
              public int compare(JobInProgress j1, JobInProgress j2) {
                double dif = weight(j2, type) - weight(j1, type);
                if (dif == 0) // Weights are equal, compare by deficit
                  dif = deficit(j2, type) - deficit(j1, type);
                return (int) Math.signum(dif);
              }
            });
            for (JobInProgress job: sortedJobs) {
              double weight = weight(job, type);
              int share = (int) Math.ceil(oldSlots * weight / totalWeight);
              slotsLeft = giveMinSlots(job, type, slotsLeft, share);
            }
            if (slotsLeft > 0) {
              LOG.warn("Had slotsLeft = " + slotsLeft + " after the final "
                  + "loop in updateMinSlots. This probably means some fair "
                  + "scheduler weights are being set to NaN or Infinity.");
            }
            break;
          }
        }
      }
    }
  }

  /**
   * Give up to <code>tasksToGive</code> min slots to a job (potentially fewer
   * if either the job needs fewer slots or there aren't enough slots left).
   * Returns the number of slots left over.
   */
  private int giveMinSlots(JobInProgress job, TaskType type,
      int slotsLeft, int slotsToGive) {
    int runnable = runnableTasks(job, type);
    int curMin = minTasks(job, type);
    slotsToGive = Math.min(Math.min(slotsLeft, runnable - curMin), slotsToGive);
    slotsLeft -= slotsToGive;
    JobInfo info = infos.get(job);
    if (type == TaskType.MAP)
      info.minMaps += slotsToGive;
    else
      info.minReduces += slotsToGive;
    return slotsLeft;
  }

  private void updateFairShares(ClusterStatus clusterStatus) {
    // Clear old fairShares
    for (JobInfo info: infos.values()) {
      info.mapFairShare = 0;
      info.reduceFairShare = 0;
    }
    // Assign new shares, based on weight and minimum share. This is done
    // as follows. First, we split up the available slots between all
    // jobs according to weight. Then if there are any jobs whose minSlots is
    // larger than their fair allocation, we give them their minSlots and
    // remove them from the list, and start again with the amount of slots
    // left over. This continues until all jobs' minSlots are less than their
    // fair allocation, and at this point we know that we've met everyone's
    // guarantee and we've split the excess capacity fairly among jobs left.
    for (TaskType type: TaskType.values()) {
      // Select only jobs that still need this type of task
      HashSet<JobInfo> jobsLeft = new HashSet<JobInfo>();
      for (Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
        JobInProgress job = entry.getKey();
        JobInfo info = entry.getValue();
        if (isRunnable(job) && runnableTasks(job, type) > 0) {
          jobsLeft.add(info);
        }
      }
      double slotsLeft = getTotalSlots(type, clusterStatus);
      while (!jobsLeft.isEmpty()) {
        double totalWeight = 0;
        for (JobInfo info: jobsLeft) {
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          totalWeight += weight;
        }
        boolean recomputeSlots = false;
        double oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
        for (Iterator<JobInfo> iter = jobsLeft.iterator(); iter.hasNext();) {
          JobInfo info = iter.next();
          double minSlots = (type == TaskType.MAP ?
              info.minMaps : info.minReduces);
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          double fairShare = weight / totalWeight * oldSlots;
          if (minSlots > fairShare) {
            // Job needs more slots than its fair share; give it its minSlots,
            // remove it from the list, and set recomputeSlots = true to
            // remember that we must loop again to redistribute unassigned slots
            if (type == TaskType.MAP)
              info.mapFairShare = minSlots;
            else
              info.reduceFairShare = minSlots;
            slotsLeft -= minSlots;
            iter.remove();
            recomputeSlots = true;
          }
        }
        if (!recomputeSlots) {
          // All minimums are met. Give each job its fair share of excess slots.
          for (JobInfo info: jobsLeft) {
            double weight = (type == TaskType.MAP ?
                info.mapWeight : info.reduceWeight);
            double fairShare = weight / totalWeight * oldSlots;
            if (type == TaskType.MAP)
              info.mapFairShare = fairShare;
            else
              info.reduceFairShare = fairShare;
          }
          break;
        }
      }
    }
  }

  private double calculateRawWeight(JobInProgress job, TaskType taskType) {
    if (!isRunnable(job)) {
      return 0;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on runnable tasks
        weight = Math.log1p(runnableTasks(job, taskType)) / Math.log(2);
      }
      weight *= getPriorityFactor(job.getPriority());
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(job, taskType, weight);
      }
      return weight;
    }
  }

  private double getPriorityFactor(JobPriority priority) {
    switch (priority) {
    case VERY_HIGH: return 4.0;
    case HIGH:      return 2.0;
    case NORMAL:    return 1.0;
    case LOW:       return 0.5;
    default:        return 0.25; // priority = VERY_LOW
    }
  }


  /**
   * Returns the LoadManager object used by the Fair Share scheduler
   */
  public LoadManager getLoadManager() {
    return loadMgr;
  }

  public PoolManager getPoolManager() {
    return poolMgr;
  }

  private int getTotalSlots(TaskType type, ClusterStatus clusterStatus) {
    return (type == TaskType.MAP ?
      clusterStatus.getMaxMapTasks() : clusterStatus.getMaxReduceTasks());
  }

  public enum JobComparator {
    DEFICIT, FAIR, FIFO;
    public static JobComparator fromString(String str) {
      if (FIFO.toString().equals(str.toUpperCase())) {
        return FIFO;
      }
      if (FAIR.toString().equals(str.toUpperCase())) {
        return FAIR;
      }
      if (DEFICIT.toString().equals(str.toUpperCase())) {
        return DEFICIT;
      }
      return null;
    }
  }
  public synchronized JobComparator getJobComparator() {
    return jobComparator;
  }

  public synchronized void setJobComparator(JobComparator jobComparator) {
    if (jobComparator != null) {
      this.jobComparator = jobComparator;
    }
  }

  // Getter methods for reading JobInfo values based on TaskType, safely
  // returning 0's for jobs with no JobInfo present.

  protected int neededTasks(JobInfo info, TaskType taskType) {
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.neededMaps : info.neededReduces;
  }

  protected int neededTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    return neededTasks (info, taskType);
  }

  protected int runningTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.runningMaps : info.runningReduces;
  }

  protected int runnableTasks(JobInProgress job, TaskType type) {
    return neededTasks(job, type) + runningTasks(job, type);
  }

  protected int minTasks(JobInProgress job, TaskType type) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (type == TaskType.MAP) ? info.minMaps : info.minReduces;
  }

  protected double weight(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (taskType == TaskType.MAP ? info.mapWeight : info.reduceWeight);
  }

  protected double deficit(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.mapDeficit : info.reduceDeficit;
  }

  protected boolean isRunnable(JobInProgress job) {
    JobInfo info = infos.get(job);
    if (info == null) return false;
    return info.runnable;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Pool myJobPool = poolMgr.getPool(queueName);
    return myJobPool.getJobs();
  }

  public int getMapPerHeartBeat() {
    return mapPerHeartBeat;
  }

  public void setMapPerHeartBeat(int mapPerHeartBeat) {
    LOG.info("The allowed Mapers per heartbeat has been changed to " +
             mapPerHeartBeat);
    this.mapPerHeartBeat = mapPerHeartBeat;
  }

  public int getReducePerHeartBeat() {
    return reducePerHeartBeat;
  }

  public void setReducePerHeartBeat(int reducePerHeartBeat) {
    LOG.info("The allowed Reducers per heartbeat has been changed to " +
             reducePerHeartBeat);
    this.reducePerHeartBeat = reducePerHeartBeat;
  }

  public void setLocalityDelayRackLocal(long localityDelay) {
    this.localityDelayRackLocal = localityDelay;
  }

  public long getLocalityDelayRackLocal() {
    return localityDelayRackLocal;
  }

  public void setLocalityDelayNodeLocal(long localityDelay) {
    this.localityDelayNodeLocal = localityDelay;
  }

  public long getLocalityDelayNodeLocal() {
    return localityDelayNodeLocal;
  }

  public boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }

  public void setPreemptionEnabled(boolean preemptionEnabled) {
    this.preemptionEnabled = preemptionEnabled;
  }

  /**
   * Update the preemption JobInfo fields for all jobs, i.e. the times since
   * each job last was at its guaranteed share and at > 1/2 of its fair share
   * for each type of task.
   */
  private void updatePreemptionVariables() {
    long now = clock.getTime();
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      if (job.getStatus().getRunState() != JobStatus.RUNNING) {
        // Job is still in PREP state and tasks aren't initialized. Count it as
        // both at min and fair share since we shouldn't start any timeouts now.
        info.lastTimeAtMapMinShare = now;
        info.lastTimeAtReduceMinShare = now;
        info.lastTimeAtMapHalfFairShare = now;
        info.lastTimeAtReduceHalfFairShare = now;
      } else {
        if (!isStarvedForMinShare(job, TaskType.MAP))
          info.lastTimeAtMapMinShare = now;
        if (!isStarvedForMinShare(job, TaskType.REDUCE))
          info.lastTimeAtReduceMinShare = now;
        if (!isStarvedForFairShare(job, TaskType.MAP))
          info.lastTimeAtMapHalfFairShare = now;
        if (!isStarvedForFairShare(job, TaskType.REDUCE))
          info.lastTimeAtReduceHalfFairShare = now;
      }
    }
  }

  /**
   * Is a job below 90% of its min share for the given task type?
   */
  boolean isStarvedForMinShare(JobInProgress job, TaskType taskType) {
    float starvingThreshold = (float) (minTasks(job, taskType) * 0.9);
    return runningTasks(job, taskType) < starvingThreshold;
  }

  /**
   * Is a job being starved for fair share for the given task type?
   * This is defined as being below half its fair share *and* having a
   * positive deficit.
   */
  boolean isStarvedForFairShare(JobInProgress job, TaskType type) {
    int desiredFairShare = (int) Math.floor(Math.min(
        fairTasks(job, type) / 2, runnableTasks(job, type)));
    return (runningTasks(job, type) < desiredFairShare);
  }

  /**
   * Check for jobs that need tasks preempted, either because they have been
   * below their guaranteed share for their pool's preemptionTimeout or they
   * have been below half their fair share for the fairSharePreemptionTimeout.
   * If such jobs exist, compute how many tasks of each type need to be
   * preempted and then select the right ones using selectTasksToPreempt.
   *
   * This method computes and logs the number of tasks we want to preempt even
   * if preemption is disabled, for debugging purposes.
   */
  protected void preemptTasksIfNecessary() {
    if (!preemptionEnabled || jobComparator == JobComparator.FIFO)
      return;

    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval)
      return;
    lastPreemptCheckTime = curTime;

    // Acquire locks on both the JobTracker (task tracker manager) and this
    // because we might need to call some JobTracker methods (killTask).
    synchronized (taskTrackerManager) {
      synchronized (this) {
        List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
        for (TaskType type: MAP_AND_REDUCE) {
          int tasksToPreempt = 0;
          for (JobInProgress job: jobs) {
            tasksToPreempt += tasksToPreempt(job, type, curTime);
          }
          // Actually preempt the tasks. The policy for this is to pick
          // tasks from jobs that are above their min share and have very
          // negative deficits (meaning they've been over-scheduled).
          // However, we also want to minimize the amount of computation
          // wasted by preemption, so prefer tasks that started recently.
          preemptTasks(jobs, type, tasksToPreempt);
        }
      }
    }
  }

  /**
   * Count how many tasks of a given type the job needs to preempt, if any.
   * If the job has been below its min share for at least its pool's preemption
   * timeout, it should preempt the difference between its current share and
   * this min share. If it has been below half its fair share for at least the
   * fairSharePreemptionTimeout, it should preempt enough tasks to get up to
   * its full fair share. If both situations hold, we preempt the max of the
   * two amounts (this shouldn't happen unless someone sets the timeouts to
   * be identical for some reason).
   */
  protected int tasksToPreempt(JobInProgress job, TaskType type, long curTime) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    String pool = poolMgr.getPoolName(job);
    long minShareTimeout = poolMgr.getMinSharePreemptionTimeout(pool);
    long fairShareTimeout = poolMgr.getFairSharePreemptionTimeout();
    int tasksDueToMinShare = 0;
    int tasksDueToFairShare = 0;
    if (type == TaskType.MAP) {
      if (curTime - info.lastTimeAtMapMinShare > minShareTimeout) {
        tasksDueToMinShare = info.minMaps - info.runningMaps;
      }
      if (curTime - info.lastTimeAtMapHalfFairShare > fairShareTimeout) {
        double fairShare = Math.min(info.mapFairShare,
                                    runnableTasks(job, type));
        tasksDueToFairShare = (int) (fairShare - info.runningMaps);
      }
    } else { // type == TaskType.REDUCE
      if (curTime - info.lastTimeAtReduceMinShare > minShareTimeout) {
        tasksDueToMinShare = info.minReduces - info.runningReduces;
      }
      if (curTime - info.lastTimeAtReduceHalfFairShare > fairShareTimeout) {
        double fairShare = Math.min(info.reduceFairShare,
                                    runnableTasks(job, type));
        tasksDueToFairShare = (int) (fairShare - info.runningReduces);
      }
    }
    int tasksToPreempt = Math.max(tasksDueToMinShare, tasksDueToFairShare);
    int neededNonSpeculativeTasks = type == TaskType.MAP ?
        info.neededMaps - info.neededSpeculativeMaps :
        info.neededReduces - info.neededSpeculativeReduces;
    // We do not preempt for speculative execution tasks
    tasksToPreempt = Math.min(neededNonSpeculativeTasks, tasksToPreempt);
    if (tasksToPreempt > 0) {
      String message = "Should preempt " + tasksToPreempt + " "
          + type + " tasks for " + job.getJobID()
          + ": tasksDueToMinShare = " + tasksDueToMinShare
          + ", tasksDueToFairShare = " + tasksDueToFairShare;
      LOG.info(message);
    }
    return tasksToPreempt < 0 ? 0 : tasksToPreempt;
  }

  /**
   * Can we preempt tasks from this job?
   */
  private boolean canBePreempted(JobInProgress job) {
    return poolMgr.canBePreempted(infos.get(job).poolName);
  }

  /**
   * Preempt up to maxToPreempt tasks of the given type.
   * Selects the tasks so as to preempt the least recently launched one first,
   * thus minimizing wasted compute time.
   */
  private void preemptTasks(Collection<JobInProgress> jobs,
                            TaskType type, int maxToPreempt) {
    if (maxToPreempt <= 0) {
      return;
    }
    Set<TaskInProgress> tips = new HashSet<TaskInProgress>();
    Map<JobInProgress, Integer> tasksCanBePreempted =
      new HashMap<JobInProgress, Integer>();
    // Collect the tasks can be preempted
    for (JobInProgress job : jobs) {
      if (!canBePreempted(job)) {
        continue;
      }
      int runningTasks = runningTasks(job, type);
      int minTasks = minTasks(job, type);
      int desiredFairShare = (int) Math.floor(Math.min(
          fairTasks(job, type), runnableTasks(job, type)));
      int tasksToLeave = Math.max(desiredFairShare, minTasks);
      int tasksCanBePreemptedCurrent = runningTasks - tasksToLeave;
      if (tasksCanBePreemptedCurrent <= 0) {
        continue;
      }
      tasksCanBePreempted.put(job, tasksCanBePreemptedCurrent);
      if (type == TaskType.MAP) {
        // Jobs may have both "non-local maps" which have a split with no
        // locality info (e.g. the input file is not in HDFS), and maps with
        // locality info, which are stored in the runningMapCache map from
        // location to task list
        tips.addAll(job.nonLocalRunningMaps);
        for (Set<TaskInProgress> set: job.runningMapCache.values()) {
          tips.addAll(set);
        }
      }
      else {
        tips.addAll(job.runningReduces);
      }
    }
    // Get the active TaskStatus'es for each TaskInProgress (there may be
    // more than one if the task has multiple copies active due to speculation)
    List<TaskStatus> statuses = new ArrayList<TaskStatus>();
    for (TaskInProgress tip: tips) {
      for (TaskAttemptID id: tip.getActiveTasks().keySet()) {
        TaskStatus stat = tip.getTaskStatus(id);
        // status is null when the task has been scheduled but not yet running
        if (stat != null) {
          statuses.add(stat);
        }
      }
    }
    // Sort the statuses in order of start time, with the latest launched first
    Collections.sort(statuses, new Comparator<TaskStatus>() {
      public int compare(TaskStatus t1, TaskStatus t2) {
        return (int) Math.signum(t2.getStartTime() - t1.getStartTime());
      }
    });
    Map<JobInProgress, Integer> tasksPreempted =
      new HashMap<JobInProgress, Integer>();
    for (TaskStatus status : statuses) {
      if (maxToPreempt <= 0) {
        break;
      }
      JobID jobId = status.getTaskID().getJobID();
      JobInProgress job = taskTrackerManager.getJob(jobId);
      if (tasksCanBePreempted.get(job) <= 0) {
        continue;
      }
      try {
        LOG.info("Preempt task: " + status.getTaskID());
        taskTrackerManager.killTask(status.getTaskID(), false);
        preemptTaskUpdateMetric(type, status.getTaskID());
        tasksCanBePreempted.put(job, tasksCanBePreempted.get(job) - 1);
        Integer count = tasksPreempted.get(job);
        if (count == null) {
          count = 0;
        }
        tasksPreempted.put(job, count + 1);
        maxToPreempt--;
      } catch (IOException e) {
        LOG.error("Failed to kill task " + status.getTaskID(), e);
      }
    }
    for (JobInProgress job : tasksPreempted.keySet()) {
      int runningTasks = runningTasks(job, type);
      int minTasks = minTasks(job, type);
      int desiredFairShare = (int) Math.floor(Math.min(
          fairTasks(job, type), runnableTasks(job, type)));
      LOG.info("Job " + job.getJobID() + " was preempted for "
               + (type == TaskType.MAP ? "map" : "reduce")
               + ": tasksPreempted = " + tasksPreempted.get(job)
               + ", fairShare = " + desiredFairShare
               + ", minSlots = " + minTasks
               + ", runningTasks = " + runningTasks);
    }
  }
  private void preemptTaskUpdateMetric(TaskType type, TaskAttemptID id) {
    if (fairSchedulerMetrics != null)
      if (type == TaskType.MAP) {
        fairSchedulerMetrics.preemptMap(id);
      } else {
        fairSchedulerMetrics.preemptReduce(id);
      }
  }
  protected double fairTasks(JobInProgress job, TaskType type) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (type == TaskType.MAP) ? info.mapFairShare : info.reduceFairShare;
  }
}
