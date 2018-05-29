/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * General configuration options pertaining to a Jet instance.
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

    private int cooperativeThreadCount = Runtime.getRuntime().availableProcessors();
    private int flowControlPeriodMs = DEFAULT_FLOW_CONTROL_PERIOD_MS;
    private int backupCount = DEFAULT_BACKUP_COUNT;

    /**
     * Sets the number of threads each cluster member will use to execute Jet
     * jobs. This refers only to threads executing <em>cooperative</em>
     * processors; each <em>blocking</em> processor is assigned its own thread.
     */
    public InstanceConfig setCooperativeThreadCount(int size) {
        checkPositive(size, "cooperativeThreadCount should be a positive number");
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
     * While executing a Jet job there is the issue of regulating the rate
     * at which one member of the cluster sends data to another member. The
     * receiver will regularly report to each sender how much more data it
     * is allowed to send over a given DAG edge. This method sets the length
     * (in milliseconds) of the interval between flow-control ("ack") packets.
     */
    public InstanceConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        checkPositive(flowControlPeriodMs, "flowControlPeriodMs should be a positive number");
        this.flowControlPeriodMs = flowControlPeriodMs;
        return this;
    }

    /**
     * Returns the {@link #setFlowControlPeriodMs(int) flow-control period} in milliseconds.
     */
    public int getFlowControlPeriodMs() {
        return flowControlPeriodMs;
    }

    /**
     * Sets the number of synchronous backups for storing job metadata and
     * snapshots. Maximum allowed value is 6, default value is 1.
     * <p>
     * For example, if backup count is set to 2, all job metadata and snapshot data
     * will be replicated to two other members. If snapshots are enabled
     * in the case that at most two members fail simultaneously the job can be restarted
     * and continued from latest snapshot.
     */
    public InstanceConfig setBackupCount(int newBackupCount) {
        checkBackupCount(newBackupCount, 0);
        this.backupCount = newBackupCount;
        return this;
    }

    /**
     * Returns the {@link #setBackupCount(int) backup-count} used for job metadata
     * and snapshots
     */
    public int getBackupCount() {
        return backupCount;
    }
}
