/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;

/**
 * General configuration options pertaining to a Jet instance.
 */
public class InstanceConfig {

    /** The default value of the {@link #setFlowControlPeriodMs(int) flow-control period}. */
    public static final int DEFAULT_FLOW_CONTROL_PERIOD_MS = 100;

    private int cooperativeThreadCount = Runtime.getRuntime().availableProcessors();
    private int flowControlPeriodMs = DEFAULT_FLOW_CONTROL_PERIOD_MS;
    private String tempDir;

    /**
     * Sets the number of threads each cluster member will use to execute Jet
     * jobs. This refers only to threads executing <em>cooperative</em>
     * processors; each <em>blocking</em> processor is assigned its own thread.
     */
    public InstanceConfig setCooperativeThreadCount(int size) {
        this.cooperativeThreadCount = size;
        return this;
    }

    /**
     * Returns the number of cooperative execution threads.
     */
    public int getCooperativeThreadCount() {
        return  cooperativeThreadCount;
    }

    /**
     * Sets the directory where Jet can place its temporary working directories.
     */
    public InstanceConfig setTempDir(String tempDir) {
        this.tempDir = tempDir;
        return this;
    }

    /**
     * Returns Jet's temp directory. Defaults to {@code java.io.tmpdir} system
     * property.
     */
    @Nonnull
    public String getTempDir() {
        return tempDir == null ? System.getProperty("java.io.tmpdir") : tempDir;
    }

    /**
     * While executing a Jet job there is the issue of regulating the rate
     * at which one member of the cluster sends data to another member. The
     * receiver will regularly report to each sender how much more data it
     * is allowed to send over a given DAG edge. This method sets the length
     * (in milliseconds) of the interval between flow-control ("ack") packets.
     */
    public InstanceConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        this.flowControlPeriodMs = flowControlPeriodMs;
        return this;
    }

    /**
     * Returns the {@link #setFlowControlPeriodMs(int) flow-control period} in milliseconds.
     */
    public int getFlowControlPeriodMs() {
        return flowControlPeriodMs;
    }
}
