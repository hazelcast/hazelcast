/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.server.FirewallingServer;
import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static org.mockito.Mockito.mock;

public class ServerAnswer
        extends AbstractAnswer {

    public ServerAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments)
            throws Exception {
        if (arguments.length == 1) {
            if (methodName.equals("getConnectionManager")) {
                arguments = proxyArgumentsIfNeeded(arguments, delegateClassloader);
                try {
                    Object endpointManager = invokeForMock(invocation, arguments);
                    return createMockForTargetClass(endpointManager, new FirewallingConnectionManagerAnswer(endpointManager));
                } catch (NoSuchMethodException e) {
                    // RU_COMPAT_4_0
                    Class<?> endpointQualifierClass = arguments[0].getClass();
                    Method method = delegate.getClass().getMethod("getEndpointManager", endpointQualifierClass);
                    Object endpointManager = invoke(false, method, arguments);
                    return mock(targetClassloader.loadClass(FirewallingServer.FirewallingServerConnectionManager.class.getName()),
                            new FirewallingConnectionManagerAnswer(endpointManager));
                }
            }
        }
        throw new UnsupportedOperationException("Not implemented: " + invocation);
    }
}
