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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.UserCodeNamespaceConfig;

/**
 * Listener to be notified about various events in {@link ClusterWideConfigurationService}
 *
 */
public interface DynamicConfigListener {
    /**
     * Called when a {@link ClusterWideConfigurationService} is initialized. It allows to hook custom hooks.
     */
    void onServiceInitialized(ClusterWideConfigurationService configurationService);

    /**
     * Called when a new {@link MapConfig} object is created locally.
     */
    void onConfigRegistered(MapConfig configObject, UserCodeNamespaceConfig ns);

    /**
     * Called when a new {@link com.hazelcast.config.CacheSimpleConfig} object is created locally.
     */
    void onConfigRegistered(CacheSimpleConfig configObject, UserCodeNamespaceConfig ns);

    /**
     * Called when a {@link com.hazelcast.config.UserCodeNamespaceConfig} object is created locally.
     */
    void onConfigRegistered(UserCodeNamespaceConfig configObject);

}
