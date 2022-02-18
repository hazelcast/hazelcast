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

package com.hazelcast.partition;

/**
 * Local statistics related to partition data replication.
 * <p>
 * Partition data are replicated across members either because
 * a new member becomes owner of a partition replica (usually the
 * term migration is used for this data replication action) or because
 * the member that owns a backup replica determined it may be
 * out of sync with the partition owner and requests partition data
 * replication.
 *
 * @since 5.0
 */
public interface LocalReplicationStats {

    /**
     * @return count of differential partition replications
     * originating from this member.
     */
    long getDifferentialPartitionReplicationCount();

    /**
     * @return count of full partition replications
     * originating from this member.
     */
    long getFullPartitionReplicationCount();

    /**
     * @return count of records replicated due to differential
     * partition replications originating from this member.
     */
    long getDifferentialReplicationRecordCount();

    /**
     * @return count of records replicated due to full
     * partition replications originating from this member.
     */
    long getFullReplicationRecordCount();

}
