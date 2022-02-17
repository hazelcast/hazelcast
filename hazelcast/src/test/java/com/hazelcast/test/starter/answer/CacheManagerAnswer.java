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

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngine;
import org.mockito.invocation.InvocationOnMock;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link javax.cache.CacheManager}.
 */
class CacheManagerAnswer extends AbstractAnswer {

    CacheManagerAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 1 && methodName.equals("getCacheByFullName")) {
            String cacheName = (String) arguments[0];
            Object original = getFieldValueReflectively(delegate, "original");
            Object delegateNode = getFieldValueReflectively(original, "node");
            Node node = mock(Node.class, new NodeAnswer(delegateNode));
            NodeEngine nodeEngine = node.getNodeEngine();
            CacheConfig cacheConfig = new CacheConfig(node.getConfig().getCacheConfig(cacheName));
            CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);

            // we have to create the mock with useConstructor(), otherwise the
            // calls to the AbstractDistributedObject (its base class) won't
            // work properly, since the NodeEngine field is not set (final
            // method calls are not mocked in the used Mockito version)
            Object cacheProxy = invokeForMock(invocation, arguments);
            return mock(CacheProxy.class, withSettings()
                    .useConstructor(cacheConfig, nodeEngine, cacheService)
                    .defaultAnswer(new CacheProxyAnswer(cacheProxy)));
        } else if (arguments.length == 0) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in CacheManagerAnswer: " + methodName);
    }
}
