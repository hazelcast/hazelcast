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

package com.hazelcast.collection.operations.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.MultiMapOperationFactory;
import com.hazelcast.spi.OperationFactory;

import java.util.Map;

/**
 * @ali 5/10/13
 */
public class SizeRequest extends CollectionAllPartitionRequest implements RetryableRequest {

    public SizeRequest() {
    }

    public SizeRequest(CollectionProxyId proxyId) {
        super(proxyId);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(proxyId, MultiMapOperationFactory.OperationFactoryType.SIZE);
    }

    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        for (Object obj: map.values()){
            total += (Integer)obj;
        }
        return total;
    }

    public int getClassId() {
        return CollectionPortableHook.SIZE;
    }
}
