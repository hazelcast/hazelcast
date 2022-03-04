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

package com.hazelcast.map.impl;

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Registry for {@code IMap} interceptors
 *
 * Interceptors are read mostly and this registry keeps all
 * registered interceptor in an array to easily iterate on them.
 * Other than that, synchronized blocks are used to prevent leaks
 * when concurrently registering/de-registering interceptors.
 * Keep in mind that all registration/de-registration operations
 * are done in generic-operation-threads, in other words,
 * synchronized methods are not used in partition-threads.
 *
 * This registry is created per map.
 *
 * @see MapInterceptor
 */
public class InterceptorRegistry {

    private volatile List<MapInterceptor> interceptors = emptyList();
    private volatile Map<String, MapInterceptor> id2InterceptorMap = emptyMap();

    /**
     * Returns all registered interceptors.
     *
     * This method is called by {@link PartitionOperationThread}
     *
     * @return all registered interceptors
     * @see com.hazelcast.map.impl.recordstore.DefaultRecordStore#put
     */
    public List<MapInterceptor> getInterceptors() {
        return interceptors;
    }

    public Map<String, MapInterceptor> getId2InterceptorMap() {
        return id2InterceptorMap;
    }

    /**
     * Registers a {@link MapInterceptor} for the supplied `id`.
     * If there is no registration associated with the `id`, registers interceptor,
     * otherwise silently ignores registration.
     *
     * This method is called by {@link com.hazelcast.spi.impl.operationexecutor.impl.GenericOperationThread}
     * when registering via {@link com.hazelcast.map.impl.operation.AddInterceptorOperation}
     *
     * @param id          ID of the interceptor
     * @param interceptor supplied {@link MapInterceptor}
     */
    public synchronized void register(String id, MapInterceptor interceptor) {
        assert !(Thread.currentThread() instanceof PartitionOperationThread);

        if (id2InterceptorMap.containsKey(id)) {
            return;
        }

        Map<String, MapInterceptor> tmpMap = new HashMap<>(id2InterceptorMap);
        tmpMap.put(id, interceptor);

        id2InterceptorMap = unmodifiableMap(tmpMap);

        List<MapInterceptor> tmpInterceptors = new ArrayList<>(interceptors);
        tmpInterceptors.add(interceptor);

        interceptors = unmodifiableList(tmpInterceptors);
    }

    /**
     * De-registers {@link MapInterceptor} for the supplied `id`, if there is any.
     *
     * This method is called by {@link com.hazelcast.spi.impl.operationexecutor.impl.GenericOperationThread}
     * when de-registering via {@link com.hazelcast.map.impl.operation.RemoveInterceptorOperation}
     *
     * @param id ID of the interceptor
     * @return {@code true} when de-registration is successful
     * otherwise returns {@code false} to indicate there is no
     * matching registration for the provided registration id
     */
    public synchronized boolean deregister(String id) {
        assert !(Thread.currentThread() instanceof PartitionOperationThread);

        if (!id2InterceptorMap.containsKey(id)) {
            return false;
        }

        Map<String, MapInterceptor> tmpMap = new HashMap<>(id2InterceptorMap);
        MapInterceptor removedInterceptor = tmpMap.remove(id);

        id2InterceptorMap = unmodifiableMap(tmpMap);

        List<MapInterceptor> tmpInterceptors = new ArrayList<>(interceptors);
        tmpInterceptors.remove(removedInterceptor);

        interceptors = unmodifiableList(tmpInterceptors);

        return true;
    }
}
