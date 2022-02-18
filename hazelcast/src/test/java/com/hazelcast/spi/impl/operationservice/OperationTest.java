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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationTest {

    // test for https://github.com/hazelcast/hazelcast/issues/11375
    @Test
    public void sendResponse_whenResponseHandlerIsNull_andThrowableValue_thenNoNPE() {
        Operation op = new DummyOperation();
        op.sendResponse(new Exception());
    }

    // test for https://github.com/hazelcast/hazelcast/issues/11375
    @Test
    public void sendResponse_whenResponseHandlerIsNull_andNoThrowableValue_thenNoNPE() {
        Operation op = new DummyOperation();
        op.sendResponse("foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowException_whenReplicaIndexInvalid() {
        Operation op = new DummyOperation();
        op.setReplicaIndex(-1);
    }
}
