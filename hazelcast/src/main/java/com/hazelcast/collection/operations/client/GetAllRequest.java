/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.GetAllOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 5/10/13
 */
public class GetAllRequest extends CollectionKeyBasedRequest {

    public GetAllRequest() {
    }

    public GetAllRequest(CollectionProxyId proxyId, Data key) {
        super(proxyId, key);
    }

    protected Operation prepareOperation() {
        return new GetAllOperation(proxyId, key);
    }

    public int getClassId() {
        return CollectionPortableHook.GET_ALL;
    }

    protected Object filter(Object response) {
        if (response instanceof CollectionResponse){
            Collection<CollectionRecord> coll = ((CollectionResponse) response).getCollection();
            Collection<Data> collection = new ArrayList<Data>(coll.size());
            for (CollectionRecord record: coll){
                collection.add(getClientEngine().toData(record.getObject()));
            }
            return new PortableCollectionResponse(collection);
        }
        return super.filter(response);
    }
}
