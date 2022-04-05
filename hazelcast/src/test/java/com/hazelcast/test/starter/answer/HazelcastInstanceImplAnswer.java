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

import com.hazelcast.cache.impl.HazelcastInstanceCacheManager;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link HazelcastInstanceImpl}.
 * <p>
 * Usage:
 * <pre><code>
 *   Object delegate = HazelcastStarter.getHazelcastInstanceImpl(hz, classloader);
 *   mock(HazelcastInstanceImpl.class, new HazelcastInstanceImplAnswer(delegate);
 * </code></pre>
 */
public class HazelcastInstanceImplAnswer extends AbstractAnswer {

    public HazelcastInstanceImplAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 0 && methodName.equals("getCacheManager")) {
            Object cacheManager = invokeForMock(invocation);
            return mock(HazelcastInstanceCacheManager.class, new CacheManagerAnswer(cacheManager));
        } else if (arguments.length == 0 && methodName.equals("getLifecycleService")) {
            Object lifecycleService = invokeForMock(invocation);
            return mock(LifecycleServiceImpl.class, new ServiceAnswer(lifecycleService));
        }
        return invoke(invocation, arguments);
    }
}
