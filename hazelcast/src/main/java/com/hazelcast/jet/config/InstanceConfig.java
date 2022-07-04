/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.config;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * General configuration options pertaining to a Jet instance.
 *
 * @since Jet 3.0
 * @deprecated since 5.0, use {@link JetConfig} instead.
 */
public class InstanceConfig {

    /**
     * The default value of the {@link #setFlowControlPeriodMs(int) flow-control period}.
     */
    public static final int DEFAULT_FLOW_CONTROL_PERIOD_MS = 100;

    /**
     * The default value of the {@link #setBackupCount(int) backup-count}
     */
    public static final int DEFAULT_BACKUP_COUNT = MapConfig.DEFAULT_BACKUP_COUNT;

    /**
     * The default value of the {@link #setScaleUpDelayMillis(long) scale up delay}.
     */
    private static final long SCALE_UP_DELAY_MILLIS_DEFAULT = SECONDS.toMillis(10);

    /**
     * The default value of the {@link #setMaxProcessorAccumulatedRecords(long) max processor accumulated records}.
     */
    private static final long MAX_PROCESSOR_ACCUMULATED_RECORDS = Long.MAX_VALUE;

    private int cooperativeThreadCount = RuntimeAvailableProcessors.get();
    private int flowControlPeriodMs = DEFAULT_FLOW_CONTROL_PERIOD_MS;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private long scaleUpDelayMillis = SCALE_UP_DELAY_MILLIS_DEFAULT;
    private boolean losslessRestartEnabled;
    private long maxProcessorAccumulatedRecords = MAX_PROCESSOR_ACCUMULATED_RECORDS;

    /**
     * Sets the number of threads each cluster member will use to execute Jet
     * jobs. This refers only to threads executing <em>cooperative</em>
     * processors; each <em>blocking</em> processor is assigned its own thread.
     */
    @Nonnull
    public InstanceConfig setCooperativeThreadCount(int size) {
        checkPositive(size, "cooperativeThreadCount should be a positive number");
        this.cooperativeThreadCount = size;
        return this;
    }

    /**
     * Returns the number of cooperative execution threads.
     */
    public int getCooperativeThreadCount() {
        return cooperativeThreadCount;
    }

    /**
     * While executing a Jet job there is the issue of regulating the rate
     * at which one member of the cluster sends data to another member. The
     * receiver will regularly report to each sender how much more data it
     * is allowed to send over a given DAG edge. This method sets the length
     * (in milliseconds) of the interval between flow-control ("ack") packets.
     */
    @Nonnull
    public InstanceConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        checkPositive(flowControlPeriodMs, "flowControlPeriodMs should be a positive number");
        this.flowControlPeriodMs = flowControlPeriodMs;
        return this;
    }

    /**
     * Returns the {@link #setFlowControlPeriodMs(int) flow-control period} in
     * milliseconds.
     */
    public int getFlowControlPeriodMs() {
        return flowControlPeriodMs;
    }

    /**
     * Sets the number of backups that Jet will maintain for the job metadata
     * and snapshots. Each backup is on another cluster member; all backup
     * write operations must complete before the overall write operation can
     * complete. The maximum allowed number of backups is 6 and the default is
     * 1.
     * <p>
     * For example, if you set the backup count to 2, Jet will replicate all
     * the job metadata and snapshot data to two other members. If one or two
     * members of the cluster fail, Jet can recover the data from the snapshot
     * and continue executing the job on the remaining members without loss.
     */
    @Nonnull
    public InstanceConfig setBackupCount(int newBackupCount) {
        checkBackupCount(newBackupCount, 0);
        this.backupCount = newBackupCount;
        return this;
    }

    /**
     * Returns the {@link #setBackupCount(int) number of backups} used for job
     * metadata and snapshots.
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the delay after which auto-scaled jobs will restart if a new member
     * is added to the cluster. The default is 10 seconds. Has no effect on
     * jobs with {@linkplain JobConfig#setAutoScaling(boolean) auto scaling}
     * disabled.
     *
     * @param millis the delay, in milliseconds
     * @return this instance for fluent API
     */
    public InstanceConfig setScaleUpDelayMillis(long millis) {
        checkNotNegative(millis, "The delay must be >=0");
        this.scaleUpDelayMillis = millis;
        return this;
    }

    /**
     * Returns the scale-up delay, see {@link #setScaleUpDelayMillis(long)}.
     */
    public long getScaleUpDelayMillis() {
        return scaleUpDelayMillis;
    }

    /**
     * Returns if lossless restart is enabled, see {@link
     * #setLosslessRestartEnabled(boolean)}.
     */
    public boolean isLosslessRestartEnabled() {
        return losslessRestartEnabled;
    }

    /**
     * Sets whether lossless job restart is enabled for the node. With lossless
     * restart you can restart the whole cluster without losing the jobs and
     * their state. The feature is implemented on top of the Hot Restart
     * feature of Hazelcast IMDG which persists the data to disk.
     * <p>
     * If enabled, you have to also configure Hot Restart:
     * <pre>{@code
     *    JetConfig jetConfig = new JetConfig();
     *    jetConfig.getInstanceConfig().setLosslessRestartEnabled(true);
     *    jetConfig.getHazelcastConfig().getHotRestartPersistenceConfig()
     *        .setEnabled(true)
     *        .setBaseDir(new File("/mnt/hot-restart"))
     *        .setParallelism(2);
     * }</pre>
     * <p>
     * Note: the snapshots exported using {@link Job#exportSnapshot}
     * will also have Hot Restart storage enabled.
     * <p>
     * Feature is disabled by default. If you enable this option in open-source
     * Hazelcast Jet, the member will fail to start, you need Jet Enterprise to
     * run it and obtain a license from Hazelcast.
     */
    public InstanceConfig setLosslessRestartEnabled(boolean enabled) {
        this.losslessRestartEnabled = enabled;
        return this;
    }

    /**
     * Sets the maximum number of records that can be accumulated by any single
     * {@link Processor} instance.
     * <p>
     * Operations like grouping, sorting or joining require certain amount of
     * records to be accumulated before they can proceed. You can set this option
     * to reduce the probability of {@link OutOfMemoryError}.
     * <p>
     * This option applies to each {@link Processor} instance separately, hence the
     * effective limit of records accumulated by each cluster member is influenced
     * by the vertex's {@code localParallelism} and the number of jobs in the cluster.
     * <p>
     * Currently, {@code maxProcessorAccumulatedRecords} limits:
     * <ul><li>
     *     number of items sorted by the sort operation
     * </li><li>
     *     number of distinct keys accumulated by aggregation operations
     * </li><li>
     *     number of entries in the hash-join lookup tables
     * </li><li>
     *     number of entries in stateful transforms
     * </li><li>
     *     number of distinct items in distinct operation
     * </li></ul>
     * <p>
     * Note: the limit does not apply to streaming aggregations.
     * <p>
     * The default value is {@link Long#MAX_VALUE}.
     *
     * @since 5.0
     */
    public InstanceConfig setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        checkPositive(maxProcessorAccumulatedRecords, "maxProcessorAccumulatedRecords must be a positive number");
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
        return this;
    }

    /**
     * Returns the maximum number of records that can be accumulated by any single
     * {@link Processor} instance.
     *
     * @since 5.0
     */
    public long getMaxProcessorAccumulatedRecords() {
        return maxProcessorAccumulatedRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InstanceConfig)) {
            return false;
        }

        InstanceConfig that = (InstanceConfig) o;

        if (cooperativeThreadCount != that.cooperativeThreadCount) {
            return false;
        }
        if (flowControlPeriodMs != that.flowControlPeriodMs) {
            return false;
        }
        if (backupCount != that.backupCount) {
            return false;
        }
        if (scaleUpDelayMillis != that.scaleUpDelayMillis) {
            return false;
        }
        if (maxProcessorAccumulatedRecords != that.maxProcessorAccumulatedRecords) {
            return false;
        }
        return losslessRestartEnabled == that.losslessRestartEnabled;
    }

    @Override
    public int hashCode() {
        int result = cooperativeThreadCount;
        result = 31 * result + flowControlPeriodMs;
        result = 31 * result + backupCount;
        result = 31 * result + (int) (scaleUpDelayMillis ^ (scaleUpDelayMillis >>> 32));
        result = 31 * result + (losslessRestartEnabled ? 1 : 0);
        result = 31 * result + (int) (maxProcessorAccumulatedRecords ^ (maxProcessorAccumulatedRecords >>> 32));
        return result;
    }
}
