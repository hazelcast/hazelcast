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

package com.hazelcast.internal.partition;

import java.util.Collection;

/**
 * Interface for {@link FragmentedMigrationAwareService}
 * implementations which may wish to offload execution of {@link
 * FragmentedMigrationAwareService#prepareReplicationOperation(PartitionReplicationEvent,
 * Collection)} method.
 * <p>
 * The default execution model is to invoke this method
 * on the partition thread.
 * <p>
 * When this interface is implemented, {@link #shouldOffload()}
 * method is consulted while preparing replication operations.
 * If {@link #shouldOffload()} returns {@code true}, then {@code
 * prepareReplicationOperation} execution is offloaded to
 * Hazelcast internal async executor instead of the partition
 * thread. This can help avoid deadlocks when replication
 * operation preparation requires network communication.
 */
public interface OffloadedReplicationPreparation {

    /**
     * @return {@code true} to offload execution of {@code prepareReplicationOperation}
     * to Hazelcast internal async executor or {@code false} to execute on
     * partition thread.
     */
    default boolean shouldOffload() {
        return false;
    }
}
