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

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * entry or item of a Hazelcast data structure, e.g.
 * {@link com.hazelcast.collection.impl.queue.QueueItem}.
 */
class DataStructureElementAnswer extends AbstractAnswer {

    DataStructureElementAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 0) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in DataStructureElementAnswer: " + methodName);
    }
}
