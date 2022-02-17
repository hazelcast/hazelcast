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
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.config.DelegatingInstanceConfig;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Configuration object for a Jet instance.
 *
 * @since Jet 3.0
 */
public class JetConfig {

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

    private final DelegatingInstanceConfig instanceConfig = new DelegatingInstanceConfig(this);

    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private boolean enabled;
    private boolean resourceUploadEnabled;
    private int cooperativeThreadCount = RuntimeAvailableProcessors.get();
    private int flowControlPeriodMs = DEFAULT_FLOW_CONTROL_PERIOD_MS;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private long scaleUpDelayMillis = SCALE_UP_DELAY_MILLIS_DEFAULT;
    private boolean losslessRestartEnabled;
    private long maxProcessorAccumulatedRecords = MAX_PROCESSOR_ACCUMULATED_RECORDS;

    /**
     * Creates a new, empty {@code JetConfig} with the default configuration.
     * Doesn't consider any configuration XML files.
     */
    public JetConfig() {
    }

    /**
     * Sets the number of threads each cluster member will use to execute Jet
     * jobs. This refers only to threads executing <em>cooperative</em>
     * processors; each <em>blocking</em> processor is assigned its own thread.
     *
     * @since 5.0
    */
    @Nonnull
    public JetConfig setCooperativeThreadCount(int size) {
        checkPositive(size, "cooperativeThreadCount should be a positive number");
        this.cooperativeThreadCount = size;
        return this;
    }

    /**
     * Returns the number of cooperative execution threads.
     *
     * @since 5.0
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
     *
     * @since 5.0
     */
    @Nonnull
    public JetConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        checkPositive(flowControlPeriodMs, "flowControlPeriodMs should be a positive number");
        this.flowControlPeriodMs = flowControlPeriodMs;
        return this;
    }

    /**
     * Returns the {@link #setFlowControlPeriodMs(int) flow-control period} in
     * milliseconds.
     *
     * @since 5.0
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
     *
     * @since 5.0
     */
    @Nonnull
    public JetConfig setBackupCount(int newBackupCount) {
        checkBackupCount(newBackupCount, 0);
        this.backupCount = newBackupCount;
        return this;
    }

    /**
     * Returns the {@link #setBackupCount(int) number of backups} used for job
     * metadata and snapshots.
     *
     * @since 5.0
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
     *
     * @since 5.0
     */
    public JetConfig setScaleUpDelayMillis(long millis) {
        checkNotNegative(millis, "The delay must be >=0");
        this.scaleUpDelayMillis = millis;
        return this;
    }

    /**
     * Returns the scale-up delay, see {@link #setScaleUpDelayMillis(long)}.
     *
     * @since 5.0
     */
    public long getScaleUpDelayMillis() {
        return scaleUpDelayMillis;
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
     *
     * @since 5.0
     */
    public JetConfig setLosslessRestartEnabled(boolean enabled) {
        this.losslessRestartEnabled = enabled;
        return this;
    }


    /**
     * Returns if lossless restart is enabled, see {@link
     * #setLosslessRestartEnabled(boolean)}.
     *
     * @since 5.0
     */
    public boolean isLosslessRestartEnabled() {
        return losslessRestartEnabled;
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
    public JetConfig setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
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

    /**
     * Returns if Jet is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets if Jet is enabled
     */
    public JetConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns if uploading resources when submitting the job enabled
     */
    public boolean isResourceUploadEnabled() {
        return resourceUploadEnabled;
    }

    /**
     * Sets if uploading resources when submitting the job enabled
     */
    public JetConfig setResourceUploadEnabled(boolean resourceUploadEnabled) {
        this.resourceUploadEnabled = resourceUploadEnabled;
        return this;
    }

    /**
     * Returns the Jet instance config.
     *
     * @deprecated The fields from {@link InstanceConfig} class were
     * moved to {@link JetConfig} class. Get the fields directly from
     * {@link JetConfig}.
     */
    @Nonnull
    @Deprecated
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     *
     * @deprecated The fields from {@link InstanceConfig} class were
     * moved to {@link JetConfig} class. Set the fields directly on
     * {@link JetConfig}.
     */
    @Nonnull
    @Deprecated
    public JetConfig setInstanceConfig(@Nonnull InstanceConfig instanceConfig) {
        Preconditions.checkNotNull(instanceConfig, "instanceConfig");
        this.instanceConfig.set(instanceConfig);
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    @Nonnull
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    @Nonnull
    public JetConfig setDefaultEdgeConfig(@Nonnull EdgeConfig defaultEdgeConfig) {
        Preconditions.checkNotNull(defaultEdgeConfig, "defaultEdgeConfig");
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JetConfig jetConfig = (JetConfig) o;

        if (enabled != jetConfig.enabled) {
            return false;
        }
        if (resourceUploadEnabled != jetConfig.resourceUploadEnabled) {
            return false;
        }
        if (!instanceConfig.equals(jetConfig.instanceConfig)) {
            return false;
        }
        return defaultEdgeConfig.equals(jetConfig.defaultEdgeConfig);
    }

    @Override
    public int hashCode() {
        int result = instanceConfig.hashCode();
        result = 31 * result + defaultEdgeConfig.hashCode();
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (resourceUploadEnabled ? 1 : 0);
        return result;
    }
}
