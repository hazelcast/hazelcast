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

import com.hazelcast.instance.LocalInstanceStats;

import java.util.Map;

public interface LocalUserCodeNamespaceStats extends LocalInstanceStats {

    /**
     * Get the map of stats for a resource of a namespace.
     *
     * @return map of resource id to resource stats.
     */
    Map<String, LocalUserCodeNamespaceResourceStats> getResources();

    /**
     *
     * @return the creation time of the stats object.
     */
    @Override
    long getCreationTime();

    /**
     *
     * @return the quantity of resources belonging to a namespace.
     */
    long getResourcesCount();
}
