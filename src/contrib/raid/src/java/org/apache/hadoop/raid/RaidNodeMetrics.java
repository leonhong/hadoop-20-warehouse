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
package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

public class RaidNodeMetrics implements Updater {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.RaidNodeMetrics");

  static long metricsResetInterval = 86400 * 1000; // 1 day.

  private static final RaidNodeMetrics instance = new RaidNodeMetrics();

  // Number of files currently raided.
  public static final String filesRaidedMetric = "files_raided";
  // Number of files fixed by block fixer.
  public static final String filesFixedMetric = "files_fixed";
  // Number of failures encountered during raiding.
  public static final String raidFailuresMetric = "raid_failures";
  // Number of purged files/directories.
  public static final String entriesPurgedMetric = "entries_purged";
  // Number of files not raided because they are too small.
  public static final String numTooSmallMetric = "num_too_small";
  // Size of files not raided because they are too small.
  public static final String sizeTooSmallMetric = "size_too_small";
  // Number of files not raided because they are too new.
  public static final String numTooNewMetric = "num_too_new";
  // Size of files not raided because they are too new.
  public static final String sizeTooNewMetric = "size_too_new";

  MetricsContext context;
  private MetricsRecord metricsRecord;
  private MetricsRegistry registry = new MetricsRegistry();
  MetricsLongValue filesRaided =
    new MetricsLongValue(filesRaidedMetric, registry);
  MetricsTimeVaryingLong raidFailures =
    new MetricsTimeVaryingLong(raidFailuresMetric, registry);
  MetricsTimeVaryingLong filesFixed =
    new MetricsTimeVaryingLong(filesFixedMetric, registry);
  MetricsTimeVaryingLong entriesPurged =
    new MetricsTimeVaryingLong(entriesPurgedMetric, registry);
  MetricsLongValue numTooSmall =
    new MetricsLongValue(numTooSmallMetric, registry);
  MetricsLongValue sizeTooSmall =
    new MetricsLongValue(sizeTooSmallMetric, registry);
  MetricsLongValue numTooNew =
    new MetricsLongValue(numTooNewMetric, registry);
  MetricsLongValue sizeTooNew =
    new MetricsLongValue(sizeTooNewMetric, registry);

  public static RaidNodeMetrics getInstance() {
    return instance;
  }

  private RaidNodeMetrics() {
    // Create a record for raid metrics
    context = MetricsUtil.getContext("raidnode");
    metricsRecord = MetricsUtil.createRecord(context, "raidnode");
    context.registerUpdater(this);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }
}
