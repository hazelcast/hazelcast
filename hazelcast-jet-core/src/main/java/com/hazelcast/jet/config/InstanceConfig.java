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

import com.hazelcast.config.MapConfig;

import javax.annotation.Nonnull;

import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;

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

    /**
     * Sets the number of synchronous backups for storing job metadata and
     * snapshots. Maximum allowed value is 6.
     * <p>
     * For example, if backup count is set to 2, all job metadata and snapshot data
     * will be replicated to two other members. If snapshots are enabled
     * in the case that at most two members fail simultaneously the job can be restarted
     * and continued from latest snapshot.
     */
    public InstanceConfig setBackupCount(int newBackupCount) {
        if (newBackupCount < 0) {
            throw new IllegalArgumentException("backup count can't be smaller than 0");
        } else if (newBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("backup count can't be larger than than " + MAX_BACKUP_COUNT);
        }
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
