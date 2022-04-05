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

package com.hazelcast.test;

import com.hazelcast.spi.impl.operationservice.Operation;

public abstract class AbstractTestOperation extends Operation {

    private static final Object NO_RESPONSE = new Object() {
        @Override
        public String toString() {
            return "NO_RESPONSE";
        }
    };

    private volatile Object response = NO_RESPONSE;

    public AbstractTestOperation(int partitionId) {
        setPartitionId(partitionId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    public boolean hasResponse() {
        return response != NO_RESPONSE;
    }

    @Override
    public void run() throws Exception {
        response = doRun();
    }

    protected abstract Object doRun();
}
