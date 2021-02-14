/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

/**
 * This class contains the definition of known Discovery SPI metadata to support automatic
 * generation of zone aware and node aware backup strategies.
 *
 * Zone aware backup strategies are based on cloud or service discovery provided information.
 * These information are split into three different levels of granularity:
 * <ul>
 * <li><b>Zone:</b> A low-latency link between (virtual) data centers in the same area</li>
 * <li><b>Rack:</b> A low-latency link inside the same data center but for different racks</li>
 * <li><b>Host:</b> A low-latency link on a shared physical node, in case of virtualization being used</li>
 * </ul>
 *
 * Node aware backup strategy is based on name of the node which is provided by container orchestration tool.
 * like Kubernetes, Docker Swarm and ECS. A node is the term used to refer machine that containers/pods run on.
 * A node may be a virtual or physical machine.
 *
 * Placement aware backup strategy is based on the placement strategies of the virtual machines on
 * which Hazelcast members run. Unlike zone aware, this strategy can group members within a single
 * availability zone based on their racks, power sources, network, etc.
 */
public enum PartitionGroupMetaData {
    ;

    /**
     * Metadata key definition for a low-latency link between (virtual) data centers in the same area
     */
    public static final String PARTITION_GROUP_ZONE = "hazelcast.partition.group.zone";

    /**
     * Metadata key definition for a low-latency link inside the same data center but for different racks.
     * This field is maintained for backward compatibility.Its use is discouraged and will be removed in a future version.
     */
    @Deprecated
    public static final String PARTITION_GROUP_RACK = "hazelcast.partition.group.rack";

    /**
     * Metadata key definition for a low-latency link on a shared physical node, in case of virtualization being used.
     * This field is maintained for backward compatibility.Its use is discouraged and will be removed in a future version.
     */
    @Deprecated
    public static final String PARTITION_GROUP_HOST = "hazelcast.partition.group.host";

    /**
     * Metadata key definition for a node machine that containers/pods run on,
     * in case of container orchestration tools being used.
     */
    public static final String PARTITION_GROUP_NODE = "hazelcast.partition.group.node";

    /**
     * Metadata key definition for the placement group to which VMs belong if a placement strategy is
     * applied by cloud providers.
     */
    public static final String PARTITION_GROUP_PLACEMENT = "hazelcast.partition.group.placement";
}
