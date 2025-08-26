/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

/**
 * Specifies the policy that will be respected during hot restart cluster start
 *
 * @deprecated use {@link PersistenceClusterDataRecoveryPolicy}
 */
@Deprecated(since = "5.0")
public enum HotRestartClusterDataRecoveryPolicy {

    /**
     * Starts the cluster only when all expected nodes are present and correct. Otherwise, it fails.
     */
    FULL_RECOVERY_ONLY,

    /**
     * Starts the cluster with the members which have most up-to-date partition table and successfully restored their data.
     * All other members will leave the cluster and force-start themselves.
     * If no member restores its data successfully, cluster start fails.
     */
    PARTIAL_RECOVERY_MOST_RECENT,

    /**
     * Starts the cluster with the largest group of members which have the same partition table version
     * and successfully restored their data. All other members will leave the cluster and force-start themselves.
     * If no member restores its data successfully, cluster start fails.
     */
    PARTIAL_RECOVERY_MOST_COMPLETE

}
