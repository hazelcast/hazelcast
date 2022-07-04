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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link NodeEngine}.
 */
public class NodeEngineAnswer extends AbstractAnswer {

    public NodeEngineAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 1 && methodName.equals("getLogger")) {
            return getLogger(arguments);
        } else if (arguments.length == 1 && methodName.equals("getService")) {
            Object service = invokeForMock(invocation, arguments);
            return createMockForTargetClass(service, new ServiceAnswer(service));
        } else if (arguments.length == 0 && methodName.equals("getOperationService")) {
            Object operationService = invokeForMock(invocation);
            return mock(OperationServiceImpl.class, new OperationServiceAnswer(operationService));
        } else if (arguments.length == 0 && methodName.equals("getConfigClassLoader")) {
            return targetClassloader;
        } else if (arguments.length == 0 && methodName.equals("getSplitBrainMergePolicyProvider")) {
            Object provider = invokeForMock(invocation, arguments);
            return createMockForTargetClass(provider, new DelegatingAnswer(provider));
        } else if (arguments.length == 0 && (methodName.startsWith("get") || methodName.startsWith("is"))) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in NodeEngineAnswer: " + methodName);
    }
}
