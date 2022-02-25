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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.CountDownLatch;

class TestMergeOperation extends Operation {

    enum OperationMode {
        NORMAL,
        THROWS_EXCEPTION,
        BLOCKS
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    private final OperationMode operationMode;

    boolean hasBeenInvoked;

    TestMergeOperation() {
        this.operationMode = OperationMode.NORMAL;
    }

    TestMergeOperation(OperationMode operationMode) {
        this.operationMode = operationMode;
    }

    public void unblock() {
        latch.countDown();
    }

    @Override
    public void run() throws Exception {
        hasBeenInvoked = true;
        switch (operationMode) {
            case THROWS_EXCEPTION:
                throw new HazelcastException("Expected exception");
            case BLOCKS:
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                break;
            default:
                // NOP
        }
    }
}
