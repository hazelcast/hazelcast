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

package com.hazelcast.map.impl;

import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Helper event listener methods for {@link MapServiceContext}.
 */
public interface MapServiceContextEventListenerSupport {

    Future<UUID> addLocalEventListener(Object mapListener, String mapName);

    Future<UUID> addLocalEventListener(Object mapListener, EventFilter eventFilter, String mapName);

    Future<UUID> addLocalPartitionLostListener(MapPartitionLostListener listener, String mapName);

    Future<UUID> addEventListener(Object mapListener, EventFilter eventFilter, String mapName);

    Future<UUID> addPartitionLostListener(MapPartitionLostListener listener, String mapName);

    Future<Boolean> removeEventListener(String mapName, UUID registrationId);

    Future<Boolean> removePartitionLostListener(String mapName, UUID registrationId);

}
