/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.operations.MultiMapOperationFactory;
import com.hazelcast.spi.OperationFactory;
import java.util.Map;

public class SizeRequest extends MultiMapAllPartitionRequest implements RetryableRequest {

    public SizeRequest() {
    }

    public SizeRequest(String name) {
        super(name);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.SIZE);
    }

    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        for (Object obj : map.values()) {
            total += (Integer) obj;
        }
        return total;
    }

    public int getClassId() {
        return MultiMapPortableHook.SIZE;
    }

    @Override
    public String getMethodName() {
        return "size";
    }
}
