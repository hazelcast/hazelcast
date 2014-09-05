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

import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;

import java.util.Collection;

import static com.hazelcast.multimap.impl.AbstractMultiMapContainerSupport.createCollection;

public class GetAllRequest extends MultiMapKeyBasedRequest implements RetryableRequest {

    public GetAllRequest() {
    }

    public GetAllRequest(String name, Data key) {
        super(name, key);
    }

    protected Operation prepareOperation() {
        return new GetAllOperation(name, key);
    }

    public int getClassId() {
        return MultiMapPortableHook.GET_ALL;
    }

    protected Object filter(Object response) {
        if (response instanceof MultiMapResponse) {
            Collection<MultiMapRecord> responseCollection = ((MultiMapResponse) response).getCollection();
            if (responseCollection == null) {
                return new PortableCollection();
            }
            Collection<Data> collection = createCollection(responseCollection);
            for (MultiMapRecord record : responseCollection) {
                collection.add(serializationService.toData(record.getObject()));
            }
            return new PortableCollection(collection);
        }
        return super.filter(response);
    }

    @Override
    public String getMethodName() {
        return "get";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }
}
