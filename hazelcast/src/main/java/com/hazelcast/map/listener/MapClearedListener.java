/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.listener;

import com.hazelcast.map.MapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.annotation.NamespacesSupported;

/**
 * Invoked after all entries are removed by {@link IMap#clear()}.
 *
 * @since 3.5
 */
@FunctionalInterface
@NamespacesSupported
public interface MapClearedListener extends MapListener {
    /**
     * Invoked when all entries are removed by {@link IMap#clear()}.
     *
     * When a listener is registered as local-only then it will be invoked if and only if
     * the <code>clear()</code> method is called on the same instance where the listener
     * was registered to.
     *
     * @param event the map event invoked when all entries are removed by {@link IMap#clear()}
     */
    void mapCleared(MapEvent event);
}
