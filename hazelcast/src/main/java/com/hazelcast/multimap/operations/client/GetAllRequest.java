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
import com.hazelcast.multimap.operations.GetAllOperation;
import com.hazelcast.multimap.operations.MultiMapResponse;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;
import java.util.ArrayList;
import java.util.Collection;

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
            Collection<MultiMapRecord> coll = ((MultiMapResponse) response).getCollection();
            if (coll == null) {
                return new PortableCollection();
            }
            Collection<Data> collection = new ArrayList<Data>(coll.size());
            for (MultiMapRecord record : coll) {
                collection.add(serializationService.toData(record.getObject()));
            }
            return new PortableCollection(collection);
        }
        return super.filter(response);
    }
}
