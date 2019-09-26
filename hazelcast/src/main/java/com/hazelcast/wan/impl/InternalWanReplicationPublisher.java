/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.wan.WanReplicationPublisher;

/**
 * Methods exposed on WAN publishers for internal use.
 */
public interface InternalWanReplicationPublisher<T> extends WanReplicationPublisher<T> {

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map name
     */
    void destroyMapData(String mapName);

    /**
     * Removes a {@code count} number of events pending replication and belonging
     * to the provided service, object and partition.
     * If the publisher does not store WAN events, this method is a no-op.
     * NOTE: used only in Hazelcast Enterprise.
     *
     * @param serviceName the service name of the WAN events should be removed
     * @param objectName  the object name of the WAN events should be removed
     * @param partitionId the partition ID of the WAN events should be removed
     * @param count       the number of events to remove
     */
    int removeWanEvents(int partitionId, String serviceName, String objectName, int count);
}
