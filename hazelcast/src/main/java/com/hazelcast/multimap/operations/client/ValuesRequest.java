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
import com.hazelcast.multimap.MultiMapRecord;
import com.hazelcast.multimap.operations.MultiMapResponse;
import com.hazelcast.multimap.operations.MultiMapOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.PortableCollection;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author ali 5/10/13
 */
public class ValuesRequest extends MultiMapAllPartitionRequest implements RetryableRequest {

    public ValuesRequest() {
    }

    public ValuesRequest(String name) {
        super(name);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.VALUES);
    }

    protected Object reduce(Map<Integer, Object> map) {
        Collection<Data> list = new LinkedList();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            MultiMapResponse response = (MultiMapResponse)obj;
            Collection<MultiMapRecord> coll = response.getCollection();
            if (coll == null){
                continue;
            }
            for (MultiMapRecord record: coll){
                list.add(getClientEngine().toData(record.getObject()));
            }
        }
        return new PortableCollection(list);
    }

    public int getClassId() {
        return MultiMapPortableHook.VALUES;
    }
}
