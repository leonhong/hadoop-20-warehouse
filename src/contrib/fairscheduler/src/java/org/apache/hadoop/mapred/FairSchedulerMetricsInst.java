package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.mapred.FairScheduler;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.conf.Configuration;


public class FairSchedulerMetricsInst implements Updater {

  private final FairScheduler scheduler;
  private final MetricsRecord metricsRecord;
  private final JobTracker jobTracker;

  private long updatePeriod = 0;
  private long lastUpdateTime = 0;

  private int numPreemptMaps = 0;
  private int numPreemptReduces = 0;

  private int numActivePools = 0;
  private int numStarvedPools = 0;
  private int totalRunningMaps = 0;
  private int totalRunningReduces = 0;
  private int totalMinReduces = 0;
  private int totalMaxReduces = 0;
  private int totalMinMaps = 0;
  private int totalMaxMaps = 0;
  private int totalRunningJobs = 0;
  private int numStarvedJobs = 0;

  public FairSchedulerMetricsInst(FairScheduler scheduler, Configuration conf) {
    this.scheduler = scheduler;
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
    // Create a record for map-reduce metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "fairscheduler");
    context.registerUpdater(this);

    updatePeriod = conf.getLong("mapred.fairscheduler.metric.update.period",
                                5 * 60 * 1000);  // default period is 5 MINs
  }

  @Override
  public void doUpdates(MetricsContext context) {
    long now = JobTracker.getClock().getTime();
    if (now - lastUpdateTime > updatePeriod) {
      updateMetrics();
      lastUpdateTime = now;
    }
    updateCounters();
    metricsRecord.update();
  }

  public synchronized void preemptMap(TaskAttemptID taskAttemptID) {
    ++numPreemptMaps;
  }

  public synchronized void preemptReduce(TaskAttemptID taskAttemptID) {
    ++numPreemptReduces;
  }

  private void updateCounters() {
    synchronized (this) {
      metricsRecord.incrMetric("num_preempt_maps", numPreemptMaps);
      metricsRecord.incrMetric("num_preempt_reduces", numPreemptReduces);

      numPreemptMaps = 0;
      numPreemptReduces = 0;
    }
  }

  private void updateMetrics() {
    synchronized (jobTracker) {
      synchronized(scheduler) {
        PoolManager poolManager = scheduler.getPoolManager();
        List<Pool> pools = new ArrayList<Pool>(poolManager.getPools());
        numActivePools = 0;
        numStarvedPools = 0;
        numStarvedJobs = 0;
        totalRunningMaps = 0;
        totalRunningReduces = 0;
        totalMinReduces = 0;
        totalMaxReduces = 0;
        totalMinMaps = 0;
        totalMaxMaps = 0;
        totalRunningJobs = 0;

        for (Pool pool: pools) {
          int runningMaps = 0;
          int runningReduces = 0;
          int neededMaps = 0;
          int neededReduces = 0;

          for (JobInProgress job: pool.getJobs()) {
            JobInfo info = scheduler.infos.get(job);
            if (info != null) {
              runningMaps += info.runningMaps;
              runningReduces += info.runningReduces;
              neededMaps += info.neededMaps + info.runningMaps;
              neededReduces += info.neededReduces + info.runningReduces;
              if ((info.neededMaps + info.runningMaps > info.mapFairShare &&
                   info.runningMaps < info.mapFairShare) ||
                  (info.neededReduces + info.runningReduces >
                   info.reduceFairShare &&
                   info.runningReduces < info.reduceFairShare)) {
                ++numStarvedJobs;
              }
            }
          }

          String poolName = pool.getName();
          int runningJobs = pool.getJobs().size();
          int minMaps = poolManager.getAllocation(poolName, TaskType.MAP);
          int minReduces = poolManager.getAllocation(poolName, TaskType.REDUCE);
          int maxMaps = poolManager.getMaxSlots(poolName, TaskType.MAP);
          int maxReduces = poolManager.getMaxSlots(poolName, TaskType.REDUCE);

          //if the pool is not active, then continue
          if (runningJobs == 0 && minMaps == 0 && minReduces == 0 &&
              maxMaps == Integer.MAX_VALUE && maxReduces == Integer.MAX_VALUE &&
              runningMaps == 0 && runningReduces == 0) {
            continue;
          }

          numActivePools++;
          if ((neededMaps > minMaps && runningMaps < minMaps) ||
              (neededReduces > minReduces && runningReduces < minReduces)) {
            ++numStarvedPools;
          }

          totalRunningJobs += runningJobs;
          totalRunningMaps += runningMaps;
          totalRunningReduces += runningReduces;
          totalMinMaps += minMaps;
          totalMinReduces += minReduces;
          if (maxMaps != Integer.MAX_VALUE) {
            totalMaxMaps += maxMaps;
          }
          if (maxReduces != Integer.MAX_VALUE) {
            totalMaxReduces += maxReduces;
          }
        }
      }
    }

    metricsRecord.setMetric("num_active_pools", numActivePools);
    metricsRecord.setMetric("num_starved_pools", numStarvedPools);
    metricsRecord.setMetric("num_starved_jobs", numStarvedJobs);
    metricsRecord.setMetric("num_running_jobs", totalRunningJobs);
    metricsRecord.setMetric("total_min_maps", totalMinMaps);
    metricsRecord.setMetric("total_max_maps", totalMaxMaps);
    metricsRecord.setMetric("total_min_reduces", totalMinReduces);
    metricsRecord.setMetric("total_max_reduces", totalMaxReduces);
  }
}
