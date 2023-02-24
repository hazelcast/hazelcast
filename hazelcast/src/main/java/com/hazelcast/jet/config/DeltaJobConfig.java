/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MetricsConfig;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableBoolean;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableBoolean;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Contains only the configurations that can be altered when the job is
 * suspended. This class stores the boxed versions of {@link JobConfig}
 * primitives, so that it is possible to determine whether a configuration
 * is set by comparing against {@code null} and clear it by specifying
 * {@code null}.
 *
 * @see Job#updateConfig(DeltaJobConfig)
 * @see JobConfig
 * @since 5.3
 */
public class DeltaJobConfig implements IdentifiedDataSerializable {
    private Long snapshotIntervalMillis;
    private Boolean autoScaling;
    private Boolean suspendOnFailure;
    private Boolean splitBrainProtectionEnabled;
    private Boolean enableMetrics;
    private Boolean storeMetricsAfterJobCompletion;
    private Long maxProcessorAccumulatedRecords;
    private Long timeoutMillis;

    // Note: new options in DeltaJobConfig must also be added to `SqlAlterJob`

    /**
     * Returns the configured {@link #setSnapshotIntervalMillis(Long) snapshot
     * interval}.
     */
    public Long getSnapshotIntervalMillis() {
        return snapshotIntervalMillis;
    }

    /**
     * Sets the snapshot interval in milliseconds &mdash; the interval between the
     * completion of the previous snapshot and the start of a new one. Must be set
     * to a positive value. This setting is only relevant with <i>at-least-once</i>
     * or <i>exactly-once</i> processing guarantees.
     * <p>
     * Default value is set to 10 seconds.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public DeltaJobConfig setSnapshotIntervalMillis(Long snapshotInterval) {
        checkTrue(snapshotInterval == null || snapshotInterval >= 0,
                "snapshotInterval can't be negative");
        this.snapshotIntervalMillis = snapshotInterval;
        return this;
    }

    /**
     * Returns whether auto-scaling is enabled, see
     * {@link #setAutoScaling(Boolean)}.
     */
    public Boolean isAutoScaling() {
        return autoScaling;
    }

    /**
     * Sets whether Jet will scale the job up or down when a member is added or
     * removed from the cluster. Enabled by default. Ignored for {@linkplain
     * JetService#newLightJob light jobs}.
     *
     * <pre>
     * +--------------------------+-----------------------+----------------+
     * |       Auto scaling       |     Member added      | Member removed |
     * +--------------------------+-----------------------+----------------+
     * | Enabled                  | restart (after delay) | restart        |
     * | Disabled - snapshots on  | no action             | suspend        |
     * | Disabled - snapshots off | no action             | fail           |
     * +--------------------------+-----------------------+----------------+
     * </pre>
     *
     * @return {@code this} instance for fluent API
     * @see InstanceConfig#setScaleUpDelayMillis
     *      Configuring the scale-up delay
     * @see JobConfig#setProcessingGuarantee
     *      Enabling/disabling snapshots
     */
    public DeltaJobConfig setAutoScaling(Boolean enabled) {
        this.autoScaling = enabled;
        return this;
    }

    /**
     * Returns whether the job will be suspended on failure, see
     * {@link #setSuspendOnFailure(Boolean)}.
     */
    public Boolean isSuspendOnFailure() {
        return suspendOnFailure;
    }

    /**
     * Sets what happens if the job execution fails: <ul>
     * <li> If enabled, the job will be suspended. It can later be {@linkplain
     *      Job#resume resumed} or upgraded and the computation state will be
     *      preserved.
     * <li> If disabled, the job will be terminated. The state snapshots will be
     *      deleted. </ul>
     * <p>
     * By default, it's disabled. Ignored for {@linkplain JetService#newLightJob
     * light jobs}.
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setSuspendOnFailure(Boolean suspendOnFailure) {
        this.suspendOnFailure = suspendOnFailure;
        return this;
    }

    /**
     * Tells whether {@link #setSplitBrainProtection(Boolean) split brain
     * protection} is enabled.
     */
    public Boolean isSplitBrainProtectionEnabled() {
        return splitBrainProtectionEnabled;
    }

    /**
     * Configures the split brain protection feature. When enabled, Jet will
     * restart the job after a topology change only if the cluster quorum is
     * satisfied. The quorum value is
     * <p>
     * {@code cluster size at job submission time / 2 + 1}.
     * <p>
     * The job can be restarted only if the size of the cluster after restart is
     * at least the quorum value. Only one of the clusters formed due to a
     * split-brain condition can satisfy the quorum. For example, if at the time
     * of job submission the cluster size was 5 and a network partition causes
     * two clusters with sizes 3 and 2 to form, the job will restart only on the
     * cluster with size 3.
     * <p>
     * Adding new nodes to the cluster after starting the job may defeat this
     * mechanism. For instance, if there are 5 members at submission time (i.e.
     * the quorum value is 3) and later a new node joins, a split into two
     * clusters of size 3 will allow the job to be restarted on both sides.
     * <p>
     * Split-brain protection is disabled by default.
     * <p>
     * If {@linkplain #setAutoScaling auto scaling} is disabled and you manually
     * {@link Job#resume} the job, the job won't start executing until the
     * quorum is met, but will remain in the resumed state.
     * <p>
     * Ignored for {@linkplain JetService#newLightJob light jobs}.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public DeltaJobConfig setSplitBrainProtection(Boolean isEnabled) {
        this.splitBrainProtectionEnabled = isEnabled;
        return this;
    }

    /**
     * Returns if metrics collection is enabled for the job.
     */
    public Boolean isMetricsEnabled() {
        return enableMetrics;
    }

    /**
     * Sets whether metrics collection should be enabled for the job. Needs
     * {@link MetricsConfig#isEnabled()} to be on in order to function.
     * <p>
     * Metrics for running jobs can be queried using {@link Job#getMetrics()}
     * It's enabled by default. Ignored for {@linkplain JetService#newLightJob
     * light jobs}.
     */
    @Nonnull
    public DeltaJobConfig setMetricsEnabled(Boolean enabled) {
        this.enableMetrics = enabled;
        return this;
    }

    /**
     * Returns whether metrics should be stored in the cluster after the job
     * completes successfully. Needs both {@link MetricsConfig#isEnabled()} and
     * {@link #isMetricsEnabled()} to be on in order to function.
     * <p>
     * If enabled, metrics can be retrieved by calling {@link
     * Job#getMetrics()}.
     * <p>
     * It's disabled by default.
     */
    public Boolean isStoreMetricsAfterJobCompletion() {
        return storeMetricsAfterJobCompletion;
    }

    /**
     * Sets whether metrics should be stored in the cluster after the job
     * completes. If enabled, metrics can be retrieved for the configured job
     * after it has completed successfully and is no longer running by calling
     * {@link Job#getMetrics()}.
     * <p>
     * If disabled, once the configured job stops running {@link
     * Job#getMetrics()} will always return empty metrics for it, regardless of
     * the settings for {@link MetricsConfig#setEnabled global metrics
     * collection} or {@link JobConfig#isMetricsEnabled() per job metrics
     * collection}.
     * <p>
     * It's disabled by default. Ignored for {@linkplain JetService#newLightJob
     * light jobs}.
     */
    public DeltaJobConfig setStoreMetricsAfterJobCompletion(Boolean storeMetricsAfterJobCompletion) {
        this.storeMetricsAfterJobCompletion = storeMetricsAfterJobCompletion;
        return this;
    }

    /**
     * Returns the maximum number of records that can be accumulated by any single
     * {@link Processor} instance in the context of the job.
     */
    public Long getMaxProcessorAccumulatedRecords() {
        return maxProcessorAccumulatedRecords;
    }

    /**
     * Sets the maximum number of records that can be accumulated by any single
     * {@link Processor} instance in the context of the job.
     * <p>
     * For more info see {@link InstanceConfig#setMaxProcessorAccumulatedRecords(long)}.
     * <p>
     * If set, it has precedence over {@link InstanceConfig}'s one.
     * <p>
     * The default value is {@code -1} - in that case {@link InstanceConfig}'s value
     * is used.
     */
    public DeltaJobConfig setMaxProcessorAccumulatedRecords(Long maxProcessorAccumulatedRecords) {
        checkTrue(maxProcessorAccumulatedRecords == null || maxProcessorAccumulatedRecords > 0
                        || maxProcessorAccumulatedRecords == -1,
                "maxProcessorAccumulatedRecords must be a positive number or -1");
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
        return this;
    }

    /**
     * Returns maximum execution time for the job in milliseconds.
     */
    public Long getTimeoutMillis() {
        return timeoutMillis;
    }

    /**
     * Sets the maximum execution time for the job in milliseconds. If the
     * execution time (counted from the time job is submitted), exceeds this
     * value, the job is forcefully cancelled. The default value is {@code 0},
     * which denotes no time limit on the execution of the job.
     */
    public DeltaJobConfig setTimeoutMillis(Long timeoutMillis) {
        checkTrue(timeoutMillis == null || timeoutMillis >= 0,
                "timeoutMillis can't be negative");
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    /**
     * Applies the changes represented by this instance to the specified
     * configuration and returns the updated configuration.
     */
    public JobConfig applyTo(JobConfig config) {
        if (snapshotIntervalMillis != null) {
            config.setSnapshotIntervalMillis(snapshotIntervalMillis);
        }
        if (autoScaling != null) {
            config.setAutoScaling(autoScaling);
        }
        if (suspendOnFailure != null) {
            config.setSuspendOnFailure(suspendOnFailure);
        }
        if (splitBrainProtectionEnabled != null) {
            config.setSplitBrainProtection(splitBrainProtectionEnabled);
        }
        if (enableMetrics != null) {
            config.setMetricsEnabled(enableMetrics);
        }
        if (storeMetricsAfterJobCompletion != null) {
            config.setStoreMetricsAfterJobCompletion(storeMetricsAfterJobCompletion);
        }
        if (maxProcessorAccumulatedRecords != null) {
            config.setMaxProcessorAccumulatedRecords(maxProcessorAccumulatedRecords);
        }
        if (timeoutMillis != null) {
            config.setTimeoutMillis(timeoutMillis);
        }
        return config;
    }

    @Override
    public int getFactoryId() {
        return JetConfigDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetConfigDataSerializerHook.DELTA_JOB_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeNullableLong(out, snapshotIntervalMillis);
        writeNullableBoolean(out, autoScaling);
        writeNullableBoolean(out, suspendOnFailure);
        writeNullableBoolean(out, splitBrainProtectionEnabled);
        writeNullableBoolean(out, enableMetrics);
        writeNullableBoolean(out, storeMetricsAfterJobCompletion);
        writeNullableLong(out, maxProcessorAccumulatedRecords);
        writeNullableLong(out, timeoutMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        snapshotIntervalMillis = readNullableLong(in);
        autoScaling = readNullableBoolean(in);
        suspendOnFailure = readNullableBoolean(in);
        splitBrainProtectionEnabled = readNullableBoolean(in);
        enableMetrics = readNullableBoolean(in);
        storeMetricsAfterJobCompletion = readNullableBoolean(in);
        maxProcessorAccumulatedRecords = readNullableLong(in);
        timeoutMillis = readNullableLong(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaJobConfig config = (DeltaJobConfig) o;
        return Objects.equals(snapshotIntervalMillis, config.snapshotIntervalMillis)
                && autoScaling == config.autoScaling
                && suspendOnFailure == config.suspendOnFailure
                && splitBrainProtectionEnabled == config.splitBrainProtectionEnabled
                && enableMetrics == config.enableMetrics
                && storeMetricsAfterJobCompletion == config.storeMetricsAfterJobCompletion
                && Objects.equals(maxProcessorAccumulatedRecords, config.maxProcessorAccumulatedRecords)
                && Objects.equals(timeoutMillis, config.timeoutMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotIntervalMillis, autoScaling, suspendOnFailure,
                splitBrainProtectionEnabled, enableMetrics, storeMetricsAfterJobCompletion,
                maxProcessorAccumulatedRecords, timeoutMillis);
    }

    @Override
    public String toString() {
        return "DeltaJobConfig {snapshotIntervalMillis=" + snapshotIntervalMillis
                + ", autoScaling=" + autoScaling
                + ", suspendOnFailure=" + suspendOnFailure
                + ", splitBrainProtectionEnabled=" + splitBrainProtectionEnabled
                + ", enableMetrics=" + enableMetrics
                + ", storeMetricsAfterJobCompletion=" + storeMetricsAfterJobCompletion
                + ", maxProcessorAccumulatedRecords=" + maxProcessorAccumulatedRecords
                + ", timeoutMillis=" + timeoutMillis + "}";
    }

    /** Assumes that the {@code value} is never {@link Long#MIN_VALUE}. */
    private static void writeNullableLong(ObjectDataOutput out, Long value) throws IOException {
        out.writeLong(value == null ? Long.MIN_VALUE : value);
    }

    /** Assumes that the value read is never {@link Long#MIN_VALUE}. */
    private static Long readNullableLong(ObjectDataInput in) throws IOException {
        long value = in.readLong();
        return value == Long.MIN_VALUE ? null : value;
    }
}
