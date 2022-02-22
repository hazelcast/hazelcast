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

package com.hazelcast.test.starter.answer;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.config.CacheConfig;
import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link com.hazelcast.cache.impl.CacheProxy}.
 */
class CacheProxyAnswer extends AbstractAnswer {

    private final Class<?> delegateHazelcastCacheManagerClass;
    private final Class<?> delegateCacheConfigClass;

    CacheProxyAnswer(Object delegate) throws Exception {
        super(delegate);
        delegateHazelcastCacheManagerClass = delegateClassloader.loadClass(HazelcastCacheManager.class.getName());
        delegateCacheConfigClass = delegateClassloader.loadClass(CacheConfig.class.getName());
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (methodName.equals("invoke") || methodName.equals("invokeAll")) {
            // TODO: we have to deal with the Object... argument variations manually for now
            if (arguments.length < 3) {
                arguments = new Object[]{arguments[0], arguments[1], new Object[]{}};
            } else if (arguments.length == 3) {
                arguments = new Object[]{arguments[0], arguments[1], new Object[]{arguments[2]}};
            }
        } else if (arguments.length == 1 && methodName.equals("setCacheManager")) {
            Object cacheManager = getDelegateMethod("getCacheManager").invoke(delegate);
            Method delegateMethod = getDelegateMethod(methodName, delegateHazelcastCacheManagerClass);
            return delegateMethod.invoke(delegate, cacheManager);
        } else if (arguments.length == 1 && methodName.equals("getConfiguration")) {
            return invoke(invocation, delegateCacheConfigClass);
        }
        return invoke(invocation, arguments);
    }
}
