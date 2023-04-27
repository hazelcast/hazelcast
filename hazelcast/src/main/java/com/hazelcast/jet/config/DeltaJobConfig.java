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

import com.hazelcast.jet.Job;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableBoolean;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableBoolean;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Contains a configuration change for {@link JobConfig}. All options are
 * initially unchanged, options for which the set-method is called will be
 * changed when this delta is {@link #applyTo(JobConfig) applied}, others will
 * be unaffected.
 * <p>
 * Contains only a subset of options - those that can be changed after the job
 * was submitted.
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
     * @see JobConfig#setSnapshotIntervalMillis(long)
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public DeltaJobConfig setSnapshotIntervalMillis(long snapshotInterval) {
        checkNotNegative(snapshotInterval, "snapshotInterval can't be negative");
        this.snapshotIntervalMillis = snapshotInterval;
        return this;
    }

    /**
     * @see JobConfig#setAutoScaling(boolean)
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setAutoScaling(boolean enabled) {
        this.autoScaling = enabled;
        return this;
    }

    /**
     * @see JobConfig#setSuspendOnFailure(boolean)
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setSuspendOnFailure(boolean suspendOnFailure) {
        this.suspendOnFailure = suspendOnFailure;
        return this;
    }

    /**
     * @see JobConfig#setSplitBrainProtection(boolean)
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public DeltaJobConfig setSplitBrainProtection(boolean isEnabled) {
        this.splitBrainProtectionEnabled = isEnabled;
        return this;
    }

    /**
     * @see JobConfig#setMetricsEnabled(boolean)
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public DeltaJobConfig setMetricsEnabled(boolean enabled) {
        this.enableMetrics = enabled;
        return this;
    }

    /**
     * @see JobConfig#setStoreMetricsAfterJobCompletion(boolean)
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setStoreMetricsAfterJobCompletion(boolean storeMetricsAfterJobCompletion) {
        this.storeMetricsAfterJobCompletion = storeMetricsAfterJobCompletion;
        return this;
    }

    /**
     * @see JobConfig#setMaxProcessorAccumulatedRecords(long)
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        checkTrue(maxProcessorAccumulatedRecords > 0 || maxProcessorAccumulatedRecords == -1,
                "maxProcessorAccumulatedRecords must be a positive number or -1");
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
        return this;
    }

    /**
     * @see JobConfig#setTimeoutMillis(long)
     *
     * @return {@code this} instance for fluent API
     */
    public DeltaJobConfig setTimeoutMillis(long timeoutMillis) {
        checkNotNegative(timeoutMillis, "timeoutMillis can't be negative");
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    /**
     * Applies the changes represented by this instance to the specified
     * configuration.
     */
    public void applyTo(JobConfig config) {
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
    }

    @Override
    @PrivateApi
    public int getFactoryId() {
        return JetConfigDataSerializerHook.FACTORY_ID;
    }

    @Override
    @PrivateApi
    public int getClassId() {
        return JetConfigDataSerializerHook.DELTA_JOB_CONFIG;
    }

    @Override
    @PrivateApi
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(snapshotIntervalMillis);
        writeNullableBoolean(out, autoScaling);
        writeNullableBoolean(out, suspendOnFailure);
        writeNullableBoolean(out, splitBrainProtectionEnabled);
        writeNullableBoolean(out, enableMetrics);
        writeNullableBoolean(out, storeMetricsAfterJobCompletion);
        out.writeObject(maxProcessorAccumulatedRecords);
        out.writeObject(timeoutMillis);
    }

    @Override
    @PrivateApi
    public void readData(ObjectDataInput in) throws IOException {
        snapshotIntervalMillis = in.readObject();
        autoScaling = readNullableBoolean(in);
        suspendOnFailure = readNullableBoolean(in);
        splitBrainProtectionEnabled = readNullableBoolean(in);
        enableMetrics = readNullableBoolean(in);
        storeMetricsAfterJobCompletion = readNullableBoolean(in);
        maxProcessorAccumulatedRecords = in.readObject();
        timeoutMillis = in.readObject();
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
}
