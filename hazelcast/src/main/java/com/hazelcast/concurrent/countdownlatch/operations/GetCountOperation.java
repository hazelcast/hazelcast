/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.spi.ReadonlyOperation;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.GET_COUNT_OPERATION;

public class GetCountOperation extends AbstractCountDownLatchOperation implements ReadonlyOperation {

    private int count;

    public GetCountOperation() {
    }

    public GetCountOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CountDownLatchService service = getService();
        count = service.getCount(name);
    }

    @Override
    public Object getResponse() {
        return count;
    }

    @Override
    public int getId() {
        return GET_COUNT_OPERATION;
    }
}
