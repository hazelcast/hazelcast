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

package com.hazelcast.cp.internal;

import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

/**
 * Marker interface for all operations used in Raft system.
 * {@code RaftSystemOperation} extends a few interfaces to bring in some extra
 * features:
 * <ul>
 * <li>{@code AllowedDuringPassiveState}: Raft operations are submitted while
 * a node is shutting down to replace shutting down member with a new one.
 * </li>
 * <li>{@code ReadonlyOperation}: Raft operations are executed on partition
 * threads but not sent to partition owners. To avoid invocations fail with
 * {@code PartitionMigratingException}, we mark them as read-only from
 * Hazelcast partitioning system perspective.
 * </li>
 * </ul>
 */
public interface RaftSystemOperation extends AllowedDuringPassiveState, ReadonlyOperation {
}
