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

import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link com.hazelcast.internal.partition.InternalPartitionService}.
 */
class PartitionServiceAnswer extends AbstractAnswer {

    PartitionServiceAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 1 && methodName.equals("getPartitionId")) {
            Method delegateMethod = getDelegateMethod(methodName, Object.class);
            return invoke(delegateMethod, arguments);
        } else if (arguments.length == 1 && methodName.equals("getPartition")) {
            return invoke(invocation, arguments);
        } else if (arguments.length == 0 && methodName.startsWith("get")) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in PartitionServiceAnswer: " + methodName);
    }
}
