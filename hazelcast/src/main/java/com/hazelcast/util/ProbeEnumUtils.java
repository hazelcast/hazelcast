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

package com.hazelcast.util;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;

/**
 * As enums are part of public API they cannot be extended to offer conversion
 * between a enum constant and a code value used as probe value to identify that
 * constant in a way that is numeric and stable over time. So this conversion
 * needs to be encoded externally what is done here.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class ProbeEnumUtils {

    private ProbeEnumUtils() {
        // utility
    }

    public static int codeOf(MemberHotRestartStatus status) {
        switch (status) {
        case FAILED: return 0;
        case PENDING: return 1;
        case LOAD_IN_PROGRESS: return 3;
        case SUCCESSFUL: return 4;
        default: return -1;
        }
    }

    public static int codeOf(BackupTaskState state) {
        switch (state) {
        case FAILURE: return 0;
        case NOT_STARTED: return 1;
        case NO_TASK: return 2;
        case IN_PROGRESS: return 3;
        case SUCCESS: return 4;
        default: return -1;
        }
    }

    public static int codeOf(ClusterHotRestartStatus status) {
        switch (status) {
        case FAILED: return 0;
        case IN_PROGRESS: return 3;
        case SUCCEEDED: return 4;
        case UNKNOWN:
        default: return -1;
        }
    }

    public static int codeOf(ClusterState state) {
        switch (state) {
        case PASSIVE: return 1;
        case FROZEN: return 2;
        case NO_MIGRATION: return 3;
        case ACTIVE: return 5;
        case IN_TRANSITION: return 6;
        default: return -1;
        }
    }

    public static int codeOf(NodeState state) {
        switch (state) {
        case PASSIVE: return 0;
        case SHUT_DOWN: return 6;
        case ACTIVE: return 5;
        default: return -1;
        }
    }

    public static int codeOf(HotRestartClusterDataRecoveryPolicy policy) {
        switch (policy) {
        case FULL_RECOVERY_ONLY: return 10;
        case PARTIAL_RECOVERY_MOST_COMPLETE: return 11;
        case PARTIAL_RECOVERY_MOST_RECENT: return 12;
        default: return -1;
        }
    }

}
