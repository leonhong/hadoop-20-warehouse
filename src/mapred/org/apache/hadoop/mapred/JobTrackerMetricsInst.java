/*
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.math.*;

import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

class JobTrackerMetricsInst extends JobTrackerInstrumentation implements Updater {
  private final MetricsRecord metricsRecord;

  private int numMapTasksLaunched = 0;
  private int numMapTasksCompleted = 0;
  private int numMapTasksFailed = 0;
  private int numReduceTasksLaunched = 0;
  private int numReduceTasksCompleted = 0;
  private int numReduceTasksFailed = 0;
  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
  private int numWaitingMaps = 0;
  private int numWaitingReduces = 0;

  private int numSpeculativeMaps = 0;
  private int numSpeculativeReduces = 0;
  private int numSpeculativeSucceededMaps = 0;
  private int numSpeculativeSucceededReduces = 0;
  private int numDataLocalMaps = 0;
  private int numRackLocalMaps = 0;

  private final Counters countersToMetrics = new Counters();

  //Cluster status fields.
  private volatile int numMapSlots = 0;
  private volatile int numReduceSlots = 0;
  private int numBlackListedMapSlots = 0;
  private int numBlackListedReduceSlots = 0;

  private int numReservedMapSlots = 0;
  private int numReservedReduceSlots = 0;
  private int numOccupiedMapSlots = 0;
  private int numOccupiedReduceSlots = 0;

  private int numJobsFailed = 0;
  private int numJobsKilled = 0;

  private int numJobsPreparing = 0;
  private int numJobsRunning = 0;

  private int numRunningMaps = 0;
  private int numRunningReduces = 0;

  private int numMapTasksKilled = 0;
  private int numReduceTasksKilled = 0;

  private int numTrackers = 0;
  private int numTrackersBlackListed = 0;
  private int numTrackersDecommissioned = 0;

  //Extended JobTracker Metrics
  private long extMetUpdatePeriod = 0;
  private long extMetLastUpdateTime = 0;
  private double totalCpu = 0;
  private double totalMemory = 0;
  private double avgSetupTime = 0;
  private long totalSubmitTime = 0;
  private long numJobsLaunched = 0;
  private double avgCpuUtil = 0;
  private double avgJobShare = 0;
  private double devJobShare = 0;
  private double covJobShare = 0;
  private long totalMapInputBytes = 0;
  private long localMapInputBytes = 0;
  private long rackMapInputBytes = 0;
  

  public JobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
    super(tracker, conf);
    String sessionId = conf.getSessionId();
    // Initiate JVM Metrics
    JvmMetrics.init("JobTracker", sessionId);
    // Create a record for map-reduce metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);

    extMetUpdatePeriod = conf.getLong(
                         "mapred.jobtrakcer.extended.metric.update.period",
                         5 * 60 * 1000);
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    // In case of running in LocalMode tracker == null
    if (tracker != null) {
      synchronized (tracker) {
        synchronized (this) {
          numRunningMaps = 0;
          numRunningReduces = 0;

          numWaitingMaps = 0;
          numWaitingReduces = 0;

          List<JobInProgress> jobs = tracker.getRunningJobs();
          for (JobInProgress jip : jobs) {
            for (TaskInProgress tip : jip.maps) {
              if (tip.isRunning()) {
                numRunningMaps++;
              } else if (tip.isRunnable()) {
                numWaitingMaps++;
              }
            }
            for (TaskInProgress tip : jip.reduces) {
              if (tip.isRunning()) {
                numRunningReduces++;
              } else if (tip.isRunnable()) {
                numWaitingReduces++;
              }

            }
          }
        }
      }
    }
    synchronized (this) {
      metricsRecord.setMetric("map_slots", numMapSlots);
      metricsRecord.setMetric("reduce_slots", numReduceSlots);
      metricsRecord.incrMetric("blacklisted_maps", numBlackListedMapSlots);
      metricsRecord.incrMetric("blacklisted_reduces",
          numBlackListedReduceSlots);
      metricsRecord.incrMetric("maps_launched", numMapTasksLaunched);
      metricsRecord.incrMetric("maps_completed", numMapTasksCompleted);
      metricsRecord.incrMetric("maps_failed", numMapTasksFailed);
      metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
      metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
      metricsRecord.incrMetric("reduces_failed", numReduceTasksFailed);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
      metricsRecord.setMetric("waiting_maps", numWaitingMaps);
      metricsRecord.setMetric("waiting_reduces", numWaitingReduces);
      metricsRecord.incrMetric("num_speculative_maps", numSpeculativeMaps);
      metricsRecord.incrMetric("num_speculative_reduces", numSpeculativeReduces);
      metricsRecord.incrMetric("num_speculative_succeeded_maps",
          numSpeculativeSucceededMaps);
      metricsRecord.incrMetric("num_speculative_succeeded_reduces",
          numSpeculativeSucceededReduces);
      metricsRecord.incrMetric("num_dataLocal_maps", numDataLocalMaps);
      metricsRecord.incrMetric("num_rackLocal_maps", numRackLocalMaps);

      metricsRecord.incrMetric("reserved_map_slots", numReservedMapSlots);
      metricsRecord.incrMetric("reserved_reduce_slots", numReservedReduceSlots);
      metricsRecord.incrMetric("occupied_map_slots", numOccupiedMapSlots);
      metricsRecord.incrMetric("occupied_reduce_slots", numOccupiedReduceSlots);

      metricsRecord.incrMetric("jobs_failed", numJobsFailed);
      metricsRecord.incrMetric("jobs_killed", numJobsKilled);

      metricsRecord.incrMetric("jobs_preparing", numJobsPreparing);
      metricsRecord.incrMetric("jobs_running", numJobsRunning);

      metricsRecord.incrMetric("running_maps", numRunningMaps);
      metricsRecord.incrMetric("running_reduces", numRunningReduces);

      metricsRecord.incrMetric("maps_killed", numMapTasksKilled);
      metricsRecord.incrMetric("reduces_killed", numReduceTasksKilled);

      metricsRecord.incrMetric("trackers", numTrackers);
      metricsRecord.incrMetric("trackers_blacklisted", numTrackersBlackListed);
      metricsRecord.setMetric("trackers_decommissioned",
          numTrackersDecommissioned);

      metricsRecord.incrMetric("num_launched_jobs", numJobsLaunched);
      metricsRecord.incrMetric("total_submit_time", totalSubmitTime);
      
      metricsRecord.incrMetric("total_map_input_bytes", totalMapInputBytes);
      metricsRecord.incrMetric("local_map_input_bytes", localMapInputBytes);
      metricsRecord.incrMetric("rack_map_input_bytes", rackMapInputBytes);

      for (Group group: countersToMetrics) {
        String groupName = group.getName();
        for (Counter counter : group) {
          String name = groupName + "_" + counter.getName();
          name = name.replaceAll("[^a-zA-Z_]", "_").toLowerCase();
          metricsRecord.incrMetric(name, counter.getValue());
        }
      }
      clearCounters();

      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numMapTasksFailed = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
      numReduceTasksFailed = 0;
      numJobsSubmitted = 0;
      numJobsCompleted = 0;
      numWaitingMaps = 0;
      numWaitingReduces = 0;
      numBlackListedMapSlots = 0;
      numBlackListedReduceSlots = 0;
      numSpeculativeMaps = 0;
      numSpeculativeReduces = 0;
      numSpeculativeSucceededMaps = 0;
      numSpeculativeSucceededReduces = 0;
      numDataLocalMaps = 0;
      numRackLocalMaps = 0;

      numReservedMapSlots = 0;
      numReservedReduceSlots = 0;
      numOccupiedMapSlots = 0;
      numOccupiedReduceSlots = 0;

      numJobsFailed = 0;
      numJobsKilled = 0;

      numJobsPreparing = 0;
      numJobsRunning = 0;

      numRunningMaps = 0;
      numRunningReduces = 0;

      numMapTasksKilled = 0;
      numReduceTasksKilled = 0;

      numTrackers = 0;
      numTrackersBlackListed = 0;

      totalSubmitTime = 0;
      numJobsLaunched = 0;
      
      totalMapInputBytes = 0;
      localMapInputBytes = 0;
      rackMapInputBytes = 0;      
    }

    long now = JobTracker.getClock().getTime();
    if ((now - extMetLastUpdateTime >= extMetUpdatePeriod) && (tracker != null)) {
      synchronized (tracker) {
        // Not syncrhonized on JobTrackerMetricsInst for a reason
        updateExtendedMetrics();
      }
    }

    metricsRecord.update();
  }

  public void updateExtendedMetrics() {
    // When getting into this method we cannot have a JobTrackerMetricsInst lock
    // because we are locking JobInProgress in this method and there is
    // another code path that locks JobInProgress and then locks the Metrics
    long now = JobTracker.getClock().getTime();

    double newTotalCpu = 0;
    double newTotalMemory = 0;
    double newAvgSetupTime = 0;
    double newAvgCpuUtil = 0;
    double newAvgJobShare = 0;
    long totalRunningJobs = 0;
    long[] totalUsedTimeOfTasks = new long[1];
    long[] partUsedTimeOfTasks = new long[1];

    long[] totalWaitTimeMapsLaunched = new long[1];
    long[] totalWaitTimeMapsPending = new long[1];
    long[] numMapsLaunched = new long[1];
    long[] numMapsPending = new long[1];
    totalWaitTimeMapsLaunched[0] = 0;
    totalWaitTimeMapsPending[0] = 0;
    numMapsLaunched[0] = 0;
    numMapsPending[0] = 0;

    List<JobInProgress> runningJobList = tracker.getRunningJobs();
    for (Iterator<JobInProgress> it = runningJobList.iterator();
         it.hasNext();) {
      JobInProgress job = it.next();
      if (!job.inited()) {
         it.remove();
      }
    }
    double [] runningJobShare = new double[runningJobList.size()];

    ResourceReporter reporter = tracker.getResourceReporter();

    int ijob = 0;
    double totalSlotsTime = (numMapSlots + numReduceSlots)
                          * (now - extMetLastUpdateTime);
    for (JobInProgress job: runningJobList) {
      // calculate the time of setup tasks
      newAvgSetupTime += getSetupTimeOfTasks(
                        tracker.getSetupTaskReports(job.getJobID()), now);

      // calculate the wait time of map tasks.
      calculateWaitTimeOfTasks(job,
                               tracker.getMapTaskReports(job.getJobID()),
                               totalWaitTimeMapsLaunched,
                               totalWaitTimeMapsPending,
                               numMapsLaunched, numMapsPending,
                               now, extMetLastUpdateTime);

      // calculate the sum of wall-time of all tasks
      totalUsedTimeOfTasks[0] = 0;
      partUsedTimeOfTasks[0] = 0;
      calculateUsedTimeOfTasks(job,
                               tracker.getSetupTaskReports(job.getJobID()),
                               totalUsedTimeOfTasks, partUsedTimeOfTasks,
                               now, extMetLastUpdateTime);
      calculateUsedTimeOfTasks(job,
                               tracker.getMapTaskReports(job.getJobID()),
                               totalUsedTimeOfTasks, partUsedTimeOfTasks,
                               now, extMetLastUpdateTime);
      calculateUsedTimeOfTasks(job,
                               tracker.getReduceTaskReports(job.getJobID()),
                               totalUsedTimeOfTasks, partUsedTimeOfTasks,
                               now, extMetLastUpdateTime);
      calculateUsedTimeOfTasks(job,
                               tracker.getCleanupTaskReports(job.getJobID()),
                               totalUsedTimeOfTasks, partUsedTimeOfTasks,
                               now, extMetLastUpdateTime);

      if (totalSlotsTime != 0) {
        runningJobShare[ijob] = (double) partUsedTimeOfTasks[0] /
                                totalSlotsTime;
      } else {
        runningJobShare[ijob] = 0;
      }
      newAvgJobShare += runningJobShare[ijob];
      ++ijob;

      // Compute the CPU and memory usage
      if (reporter != null) {
        double cpuUse = reporter.getJobCpuCumulatedUsageTime(job.getJobID());
        if (cpuUse != ResourceReporter.UNAVAILABLE &&
            totalUsedTimeOfTasks[0] != 0) {
          newAvgCpuUtil += cpuUse / totalUsedTimeOfTasks[0];
        }

        double cpu = reporter.getJobCpuPercentageOnCluster(job.getJobID());
        double memory = reporter.getJobMemPercentageOnCluster(job.getJobID());
        newTotalCpu += cpu != ResourceReporter.UNAVAILABLE ? cpu : 0;
        newTotalMemory += memory != ResourceReporter.UNAVAILABLE ? memory : 0;
      }
    }
    totalRunningJobs = runningJobList.size();

    double newDevJobShare = devJobShare;
    double newCovJobShare = covJobShare; 
    if (totalRunningJobs != 0) {
      newAvgCpuUtil /= totalRunningJobs;
      newAvgSetupTime /= totalRunningJobs;
      newAvgJobShare /= totalRunningJobs;
      newDevJobShare = 0;
      if (runningJobShare != null) {
        for (int i = 0; i < runningJobShare.length; ++i)
          newDevJobShare += (runningJobShare[i] - newAvgJobShare) *
                         (runningJobShare[i] - newAvgJobShare);
      }
      newDevJobShare = Math.sqrt(newDevJobShare / totalRunningJobs);
      if (newAvgJobShare != 0) {
        newCovJobShare = newDevJobShare / newAvgJobShare;
      } else {
        newCovJobShare = 0;
      }
    }
    synchronized (this) {
      totalCpu = newTotalCpu;
      totalMemory = newTotalMemory;
      avgCpuUtil = newAvgCpuUtil;
      avgJobShare = newAvgJobShare;
      devJobShare = newDevJobShare;
      covJobShare = newCovJobShare;
      
      metricsRecord.setMetric("total_cpu", (float) totalCpu);
      metricsRecord.setMetric("total_memory", (float) totalMemory);
      metricsRecord.setMetric("avg_cpu_utilization", (float) avgCpuUtil);
      metricsRecord.setMetric("avg_waittime_maps_launched",
        (float) totalWaitTimeMapsLaunched[0] / numMapsLaunched[0] / 1000);
      metricsRecord.setMetric("avg_waittime_maps_pending",
        (float) totalWaitTimeMapsPending[0] / numMapsPending[0] / 1000);
      metricsRecord.setMetric("avg_waittime_maps",
        (float) (totalWaitTimeMapsLaunched[0] + totalWaitTimeMapsPending[0])
              / (numMapsLaunched[0] + numMapsPending[0]) / 1000);
      metricsRecord.setMetric("avg_setup_time", (float) avgSetupTime / 1000);
      metricsRecord.setMetric("num_running_jobs_share", (float) totalRunningJobs);
      metricsRecord.setMetric("avg_job_share", (float) avgJobShare);
      metricsRecord.setMetric("dev_job_share", (float) devJobShare);
      metricsRecord.setMetric("cov_job_share", (float) covJobShare);
      
      extMetLastUpdateTime = now;
      
    }
  }

  private long getSetupTimeOfTasks(TaskReport[] reports, long now) {
    if (reports.length == 0)
        return 0;
    int nSetupTasks = 0;
    long duration = 0;
    for (int i = 0; i < reports.length; i++) {
      TaskReport report = reports[i];
      if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
        duration = report.getFinishTime() - report.getStartTime();
        ++nSetupTasks;
      } else
      if (report.getCurrentStatus() == TIPStatus.RUNNING) {
        duration = now - report.getStartTime();
        ++nSetupTasks;
      }
    }
    if (nSetupTasks > 1) {
      return 0;
    } else {
      return duration;
    }
  }

  private void calculateWaitTimeOfTasks(JobInProgress job,
                                        TaskReport[] reports,
                                        long totalWaitTimeOfLaunchedTasks[],
                                        long totalWaitTimeOfPendingTasks[],
                                        long numLaunchedTasks[],
                                        long numPendingTasks[],
                                        long now, long last) {
    long aggrWaitLaunched = 0;
    long aggrWaitPending = 0;
    long numPending = 0;
    long numLaunched = 0;
    for (int i = 0; i < reports.length; i++) {
      TaskReport report = reports[i];
      if (report.getCurrentStatus() == TIPStatus.PENDING) {
        aggrWaitPending += (now - job.getLaunchTime());
        numPending++;
      } else if ((report.getCurrentStatus() == TIPStatus.COMPLETE ||
        report.getCurrentStatus() == TIPStatus.RUNNING) &&
        report.getFinishTime() >= last) {
        aggrWaitLaunched += (report.getStartTime() - job.getLaunchTime());
        numLaunched++;
      }
    }
    totalWaitTimeOfLaunchedTasks[0] += aggrWaitLaunched;
    totalWaitTimeOfPendingTasks[0] += aggrWaitPending;
    numLaunchedTasks[0] += numLaunched;
    numPendingTasks[0] += numPending;
  }

  private void calculateUsedTimeOfTasks(JobInProgress job,
                                        TaskReport[] reports,
                                        long totalUsedTimeOfTasks[],
                                        long partUsedTimeOfTasks[],
                                        long now, long last) {
    long aggrUsed = 0;
    long aggrPartUsed = 0;
    for (int i = 0; i < reports.length; i++) {
      TaskReport report = reports[i];
      if (report.getCurrentStatus() == TIPStatus.COMPLETE ||
        report.getCurrentStatus() == TIPStatus.KILLED) {
        aggrUsed += report.getFinishTime() - report.getStartTime();
        if (report.getFinishTime() > last) {
          aggrPartUsed += report.getFinishTime() - last;
        }
      } else if (report.getCurrentStatus() == TIPStatus.RUNNING) {
        aggrUsed += now - report.getStartTime();
        if (report.getStartTime() > last)
          aggrPartUsed += now - report.getStartTime();
        else
          aggrPartUsed += now - last;
      }
    }
    totalUsedTimeOfTasks[0] += aggrUsed;
    partUsedTimeOfTasks[0] += aggrPartUsed;
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }
  @Override
  public synchronized void launchDataLocalMap(TaskAttemptID taskAttemptID) {
    ++numDataLocalMaps;
  }
  @Override
  public synchronized void launchRackLocalMap(TaskAttemptID taskAttemptID) {
    ++numRackLocalMaps;
  }

  @Override
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }

  @Override
  public synchronized void speculateMap(TaskAttemptID taskAttemptID) {
    ++numSpeculativeMaps;
  }

  public synchronized void speculativeSucceededMap(
          TaskAttemptID taskAttemptID) {
    ++numSpeculativeSucceededMaps;
  }

  public synchronized void speculativeSucceededReduce(
          TaskAttemptID taskAttemptID) {
    ++numSpeculativeSucceededReduces;
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksFailed;
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }

  @Override
  public synchronized void speculateReduce(TaskAttemptID taskAttemptID) {
    ++numSpeculativeReduces;
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksFailed;
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void submitJob(JobConf conf, JobID id) {
    ++numJobsSubmitted;
  }

  @Override
  public synchronized void completeJob(JobConf conf, JobID id) {
    collectJobCounters(id);
    ++numJobsCompleted;
  }

  @Override
  public synchronized void addWaitingMaps(JobID id, int task) {
  }

  @Override
  public synchronized void decWaitingMaps(JobID id, int task) {
  }

  @Override
  public synchronized void addWaitingReduces(JobID id, int task) {
  }

  @Override
  public synchronized void decWaitingReduces(JobID id, int task){
  }

  @Override
  public synchronized void setMapSlots(int slots) {
    numMapSlots = slots;
  }

  @Override
  public synchronized void setReduceSlots(int slots) {
    numReduceSlots = slots;
  }

  @Override
  public synchronized void addBlackListedMapSlots(int slots){
    numBlackListedMapSlots += slots;
  }

  @Override
  public synchronized void decBlackListedMapSlots(int slots){
    numBlackListedMapSlots -= slots;
  }

  @Override
  public synchronized void addBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots += slots;
  }

  @Override
  public synchronized void decBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots -= slots;
  }

  @Override
  public synchronized void addReservedMapSlots(int slots)
  {
    numReservedMapSlots += slots;
  }

  @Override
  public synchronized void decReservedMapSlots(int slots)
  {
    numReservedMapSlots -= slots;
  }

  @Override
  public synchronized void addReservedReduceSlots(int slots)
  {
    numReservedReduceSlots += slots;
  }

  @Override
  public synchronized void decReservedReduceSlots(int slots)
  {
    numReservedReduceSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots += slots;
  }

  @Override
  public synchronized void decOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots += slots;
  }

  @Override
  public synchronized void decOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots -= slots;
  }

  @Override
  public synchronized void failedJob(JobConf conf, JobID id)
  {
    numJobsFailed++;
  }

  @Override
  public synchronized void killedJob(JobConf conf, JobID id)
  {
    numJobsKilled++;
  }

  @Override
  public synchronized void addPrepJob(JobConf conf, JobID id)
  {
    numJobsPreparing++;
  }

  @Override
  public synchronized void decPrepJob(JobConf conf, JobID id)
  {
    numJobsPreparing--;
  }

  @Override
  public synchronized void addRunningJob(JobConf conf, JobID id)
  {
    numJobsRunning++;
  }

  @Override
  public synchronized void decRunningJob(JobConf conf, JobID id)
  {
    numJobsRunning--;
  }

  @Override
  public synchronized void addRunningMaps(int task)
  {
  }

  @Override
  public synchronized void decRunningMaps(int task)
  {
  }

  @Override
  public synchronized void addRunningReduces(int task)
  {
  }

  @Override
  public synchronized void decRunningReduces(int task)
  {
  }

  @Override
  public synchronized void killedMap(TaskAttemptID taskAttemptID)
  {
    numMapTasksKilled++;
  }

  @Override
  public synchronized void killedReduce(TaskAttemptID taskAttemptID)
  {
    numReduceTasksKilled++;
  }

  @Override
  public synchronized void addTrackers(int trackers)
  {
    numTrackers += trackers;
  }

  @Override
  public synchronized void decTrackers(int trackers)
  {
    numTrackers -= trackers;
  }

  @Override
  public synchronized void addBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed += trackers;
  }

  @Override
  public synchronized void decBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed -= trackers;
  }

  @Override
  public synchronized void setDecommissionedTrackers(int trackers)
  {
    numTrackersDecommissioned = trackers;
  }

  @Override
  public synchronized void addLaunchedJobs(long submitTime)
  {
    ++numJobsLaunched;
    totalSubmitTime += submitTime;
  }

  @Override
  public synchronized void addMapInputBytes(long size) {
    totalMapInputBytes += size;
  }

  @Override
  public synchronized void addLocalMapInputBytes(long size) {
    localMapInputBytes += size;
    addMapInputBytes(size);
  }

  @Override
  public synchronized void addRackMapInputBytes(long size) {
    rackMapInputBytes += size;
    addMapInputBytes(size);
  }

  @Override
  public void terminateJob(JobConf conf, JobID id) {
    collectJobCounters(id);
  }

  private synchronized void collectJobCounters(JobID id) {
    JobInProgress job = tracker.jobs.get(id);
    Counters jobCounter = job.getCounters();
    for (JobInProgress.Counter key : JobInProgress.Counter.values()) {
      countersToMetrics.findCounter(key).
      increment(jobCounter.findCounter(key).getValue());
    }
    for (Task.Counter key : Task.Counter.values()) {
      countersToMetrics.findCounter(key).
      increment(jobCounter.findCounter(key).getValue());
    }
    for (Counter counter : jobCounter.getGroup(Task.FILESYSTEM_COUNTER_GROUP)) {
      countersToMetrics.incrCounter(
          Task.FILESYSTEM_COUNTER_GROUP, counter.getName(), counter.getValue());
    }
  }
  /*
   *  Set everything in the counters to zero
   */
  private void clearCounters() {
    for (Group g : countersToMetrics) {
      for (Counter c : g) {
        c.setValue(0);
      }
    }
  }
}
