/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * generation of zone aware backup strategies based on cloud or service discovery provided
 * information. These information are split into three different levels of granularity:
 * <ul>
 * <li><b>Zone:</b> A low-latency link between (virtual) data centers in the same area</li>
 * <li><b>Rack:</b> A low-latency link inside the same data center but for different racks</li>
 * <li><b>Host:</b> A low-latency link on a shared physical node, in case of virtualization being used</li>
 * </ul>
 */
public enum PartitionGroupMetaData {
    ;

    /**
     * Metadata key definition for a low-latency link between (virtual) data centers in the same area
     */
    public static final String PARTITION_GROUP_ZONE = "hazelcast.partition.group.zone";

    /**
     * Metadata key definition for a low-latency link inside the same data center but for different racks
     */
    public static final String PARTITION_GROUP_RACK = "hazelcast.partition.group.rack";

    /**
     * Metadata key definition for a low-latency link on a shared physical node, in case of virtualization being used
     */
    public static final String PARTITION_GROUP_HOST = "hazelcast.partition.group.host";
}
