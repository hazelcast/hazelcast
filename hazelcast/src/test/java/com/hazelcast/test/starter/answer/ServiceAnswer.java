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

package com.hazelcast.test.starter.answer;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.multimap.impl.MultiMapPartitionContainer;
import com.hazelcast.internal.services.ObjectNamespace;
import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * Hazelcast {@code Service}.
 */
class ServiceAnswer extends AbstractAnswer {

    private final Class<?> objectNamespaceClass;
    private final Class<?> preJoinCacheConfigClass;
    private final Class<?> cacheConfigClass;
    private final Class<?> inMemoryFormatClass;

    ServiceAnswer(Object delegate) throws Exception {
        super(delegate);
        objectNamespaceClass = delegateClassloader.loadClass(ObjectNamespace.class.getName());
        preJoinCacheConfigClass = delegateClassloader.loadClass(PreJoinCacheConfig.class.getName());
        cacheConfigClass = delegateClassloader.loadClass(CacheConfig.class.getName());
        inMemoryFormatClass = delegateClassloader.loadClass(InMemoryFormat.class.getName());
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 2 && methodName.equals("getContainerOrNull")) {
            // RingbufferService
            Method delegateMethod = getDelegateMethod(methodName, Integer.TYPE, objectNamespaceClass);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 2 && methodName.equals("getOrCreateContainer")) {
            // QueueService
            Method delegateMethod = getDelegateMethod(methodName, String.class, Boolean.TYPE);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 1 && (methodName.equals("getLongContainer")
                || methodName.equals("getReferenceContainer")
                || methodName.equals("getCardinalityEstimatorContainer"))) {
            // AtomicLongService, AtomicReferenceService, CardinalityEstimatorService
            Method delegateMethod = getDelegateMethod(methodName, String.class);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 1 && methodName.equals("getPartitionContainer")) {
            // MultiMapService
            Method delegateMethod = getDelegateMethod(methodName, Integer.TYPE);
            return getMultiMapPartitionContainer(delegateMethod, arguments);
        } else if (arguments.length == 2 && methodName.equals("getCacheOperationProvider")) {
            // CacheService
            Method delegateMethod = delegateClass.getMethod(methodName, String.class, inMemoryFormatClass);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 2 && methodName.equals("getRecordStore")) {
            // CacheService
            Method delegateMethod = delegateClass.getMethod(methodName, String.class, Integer.TYPE);
            return getICacheRecordStore(delegateMethod, arguments);
        } else if (arguments.length == 1 && (methodName.equals("getCacheConfig")
                || methodName.equals("findCacheConfig"))) {
            // CacheService
            return invoke(invocation, arguments);
        } else if (arguments.length == 1 && methodName.equals("createCacheConfigOnAllMembers")) {
            // CacheService
            Method delegateMethod = delegateClass.getMethod(methodName, preJoinCacheConfigClass);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 1 && methodName.equals("putCacheConfigIfAbsent")) {
            // CacheService
            Method delegateMethod = delegateClass.getMethod(methodName, cacheConfigClass);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 1 && methodName.equals("deleteCacheConfig")) {
            // CacheService
            return invoke(invocation, arguments);
        } else if (arguments.length == 1 && (methodName.equals("addLifecycleListener"))) {
            // LifecycleService
            // FIXME
            return null;
        } else if (arguments.length == 1 && (methodName.equals("removeLifecycleListener"))) {
            // LifecycleService
            // FIXME
            return null;
        } else if (arguments.length == 0 && methodName.equals("getMapServiceContext")) {
            // MapService
            Object mapServiceContext = invokeForMock(invocation);
            return mock(MapServiceContext.class, new MapServiceContextAnswer(mapServiceContext));
        } else if (arguments.length == 0 && methodName.startsWith("isRunning")) {
            // LifecycleService
            return invoke(invocation);
        } else if (arguments.length == 0 && methodName.startsWith("get")) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in ServiceAnswer: " + methodName);
    }

    private Object getMultiMapPartitionContainer(Method delegateMethod, Object[] arguments) throws Exception {
        Object container = delegateMethod.invoke(delegate, arguments);
        if (container == null) {
            return null;
        }
        return mock(MultiMapPartitionContainer.class, new PartitionContainerAnswer(container));
    }

    private Object getICacheRecordStore(Method delegateMethod, Object[] arguments) throws Exception {
        Object recordStore = delegateMethod.invoke(delegate, arguments);
        if (recordStore == null) {
            return null;
        }
        return mock(ICacheRecordStore.class, new RecordStoreAnswer(recordStore));
    }
}
