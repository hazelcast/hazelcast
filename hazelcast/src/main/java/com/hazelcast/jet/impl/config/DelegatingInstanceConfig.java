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

package com.hazelcast.jet.impl.config;

import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;

/**
 * This class is used to delegate the deprecated {@link InstanceConfig} method
 * calls to {@link JetConfig}.
 */
public final class DelegatingInstanceConfig extends InstanceConfig {

    private final JetConfig jetConfig;

    public DelegatingInstanceConfig(JetConfig jetConfig) {
        this.jetConfig = jetConfig;
    }

    @Nonnull
    @Override
    public InstanceConfig setCooperativeThreadCount(int size) {
        super.setCooperativeThreadCount(size);
        jetConfig.setCooperativeThreadCount(size);
        return this;
    }

    @Override
    public int getCooperativeThreadCount() {
        return jetConfig.getCooperativeThreadCount();
    }

    @Nonnull
    @Override
    public InstanceConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        super.setFlowControlPeriodMs(flowControlPeriodMs);
        jetConfig.setFlowControlPeriodMs(flowControlPeriodMs);
        return this;
    }

    @Override
    public int getFlowControlPeriodMs() {
        return jetConfig.getFlowControlPeriodMs();
    }

    @Nonnull
    @Override
    public InstanceConfig setBackupCount(int newBackupCount) {
        super.setBackupCount(newBackupCount);
        jetConfig.setBackupCount(newBackupCount);
        return this;
    }

    @Override
    public int getBackupCount() {
        return jetConfig.getBackupCount();
    }

    @Override
    public InstanceConfig setScaleUpDelayMillis(long millis) {
        super.setScaleUpDelayMillis(millis);
        jetConfig.setScaleUpDelayMillis(millis);
        return this;
    }

    @Override
    public long getScaleUpDelayMillis() {
        return jetConfig.getScaleUpDelayMillis();
    }

    @Override
    public InstanceConfig setLosslessRestartEnabled(boolean enabled) {
        super.setLosslessRestartEnabled(enabled);
        jetConfig.setLosslessRestartEnabled(enabled);
        return this;
    }

    @Override
    public boolean isLosslessRestartEnabled() {
        return jetConfig.isLosslessRestartEnabled();
    }

    @Override
    public InstanceConfig setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        super.setMaxProcessorAccumulatedRecords(maxProcessorAccumulatedRecords);
        jetConfig.setMaxProcessorAccumulatedRecords(maxProcessorAccumulatedRecords);
        return this;
    }

    @Override
    public long getMaxProcessorAccumulatedRecords() {
        return jetConfig.getMaxProcessorAccumulatedRecords();
    }

    public void set(InstanceConfig instanceConfig) {
        setMaxProcessorAccumulatedRecords(instanceConfig.getMaxProcessorAccumulatedRecords());
        setLosslessRestartEnabled(instanceConfig.isLosslessRestartEnabled());
        setScaleUpDelayMillis(instanceConfig.getScaleUpDelayMillis());
        setBackupCount(instanceConfig.getBackupCount());
        setFlowControlPeriodMs(instanceConfig.getFlowControlPeriodMs());
        setCooperativeThreadCount(instanceConfig.getCooperativeThreadCount());
    }
}
