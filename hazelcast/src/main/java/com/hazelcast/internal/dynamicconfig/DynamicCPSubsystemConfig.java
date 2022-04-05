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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;

import java.io.File;
import java.util.Map;

/**
 * {@link CPSubsystemConfig} wrapper that disables updates to config object of a Hazelcast instance
 */
class DynamicCPSubsystemConfig extends CPSubsystemConfig {

    DynamicCPSubsystemConfig(CPSubsystemConfig config) {
        super(config);
    }

    @Override
    public CPSubsystemConfig setCPMemberCount(int cpMemberCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setGroupSize(int groupSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setSessionTimeToLiveSeconds(int sessionTimeToLiveSeconds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setSessionHeartbeatIntervalSeconds(int sessionHeartbeatIntervalSeconds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setMissingCPMemberAutoRemovalSeconds(int missingCPMemberAutoRemovalSeconds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return new DynamicRaftAlgorithmConfig(super.getRaftAlgorithmConfig());
    }

    @Override
    public CPSubsystemConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig addLockConfig(FencedLockConfig lockConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setLockConfigs(Map<String, FencedLockConfig> lockConfigs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setPersistenceEnabled(boolean persistenceEnabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setBaseDir(File baseDir) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemConfig setDataLoadTimeoutSeconds(int dataLoadTimeoutSeconds) {
        throw new UnsupportedOperationException();
    }

    static class DynamicRaftAlgorithmConfig extends RaftAlgorithmConfig {

        DynamicRaftAlgorithmConfig(RaftAlgorithmConfig config) {
            super(config);
        }

        @Override
        public RaftAlgorithmConfig setLeaderElectionTimeoutInMillis(long leaderElectionTimeoutInMillis) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RaftAlgorithmConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RaftAlgorithmConfig setAppendRequestMaxEntryCount(int appendRequestMaxEntryCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RaftAlgorithmConfig setCommitIndexAdvanceCountToSnapshot(int commitIndexAdvanceCountToSnapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RaftAlgorithmConfig setUncommittedEntryCountToRejectNewAppends(int uncommittedEntryCountToRejectNewAppends) {
            throw new UnsupportedOperationException();
        }
    }

}
