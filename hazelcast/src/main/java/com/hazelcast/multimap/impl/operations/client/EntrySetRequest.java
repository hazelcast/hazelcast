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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.operations.EntrySetResponse;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EntrySetRequest extends MultiMapAllPartitionRequest implements RetryableRequest {

    public EntrySetRequest() {
    }

    public EntrySetRequest(String name) {
        super(name);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.ENTRY_SET);
    }

    protected Object reduce(Map<Integer, Object> map) {
        Set<Map.Entry> entrySet = new HashSet<Map.Entry>();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            EntrySetResponse response = (EntrySetResponse) obj;
            Set<Map.Entry<Data, Data>> entries = response.getDataEntrySet();
            entrySet.addAll(entries);
        }
        return new PortableEntrySetResponse(entrySet);
    }

    public int getClassId() {
        return MultiMapPortableHook.ENTRY_SET;
    }

    @Override
    public String getMethodName() {
        return "entrySet";
    }
}
