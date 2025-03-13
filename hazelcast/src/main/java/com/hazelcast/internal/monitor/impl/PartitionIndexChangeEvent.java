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

package com.hazelcast.internal.monitor.impl;

/**
 * Monitoring events emitted by an index when the partition set it covers changes. It
 * is possible for a CHANGE_STARTED and CHANGE_FINISHED pair to be emitted without a
 * change to the underlying partition set. For example if a index is requested to
 * index a partition it already has in the set.
 */
public enum PartitionIndexChangeEvent {
    /**
     * A partition set change has started
     */
    CHANGE_STARTED,
    /**
     * A new partition has been indexed and added to the set
     */
    INDEXED,
    /**
     * An existing partition has been unindexed and removed from the set
     */
    UNINDEXED,
    /**
     * A partition set change has finished.
     */
    CHANGE_FINISHED
}
