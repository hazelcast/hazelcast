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

package com.hazelcast.spi.partitiongroup;

import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;

/**
 * <p>A <code>PartitionGroupStrategy</code> implementation defines a strategy
 * how backup groups are designed. Backup groups are units containing
 * one or more Hazelcast nodes to share the same physical node/machine or
 * zone and backups are stored on nodes being part of a different
 * backup group. This behavior builds an additional layer of data
 * reliability by making sure that, in case of two zones, if zone A
 * fails, zone B will still have all the backups and is guaranteed
 * to still provide all data. Similar is true for nodes or physical hosts.</p>
 * <p>Custom implementations of the PartitionGroupStrategy may add specific
 * or additional behavior based on the provided environment and can
 * be injected into Hazelcast by overriding
 * {@link AbstractDiscoveryStrategy#getPartitionGroupStrategy()}. </p>
 */
@FunctionalInterface
public interface PartitionGroupStrategy {

    Iterable<MemberGroup> getMemberGroups();
}
