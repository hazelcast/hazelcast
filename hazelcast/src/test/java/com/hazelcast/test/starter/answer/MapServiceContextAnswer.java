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

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link MapServiceContext}.
 */
class MapServiceContextAnswer extends AbstractAnswer {

    MapServiceContextAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 1 && methodName.equals("getPartitionContainer")) {
            return getPartitionContainer(methodName, arguments);
        }
        throw new UnsupportedOperationException("Method is not implemented in MapServiceContextAnswer: " + methodName);
    }

    private Object getPartitionContainer(String methodName, Object[] arguments) throws Exception {
        Method delegateMethod = getDelegateMethod(methodName, Integer.TYPE);
        Object container = delegateMethod.invoke(delegate, arguments);
        if (container == null) {
            return null;
        }
        return mock(PartitionContainer.class, new PartitionContainerAnswer(container));
    }
}
