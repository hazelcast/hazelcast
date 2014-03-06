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

package com.hazelcast.multimap.operations;

import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ResponseHandler;

import java.util.Collection;

/**
 * @author ali 1/16/13
 */
public class GetAllOperation extends MultiMapKeyBasedOperation {

    public GetAllOperation() {
    }

    public GetAllOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() throws Exception {
        MultiMapWrapper wrapper = getOrCreateContainer().getMultiMapWrapper(dataKey);
        Collection coll = null;
        if (wrapper != null) {
            wrapper.incrementHit();
            final ResponseHandler responseHandler = getResponseHandler();
            coll = wrapper.getCollection(responseHandler.isLocal());
        }
        response = new MultiMapResponse(coll);
    }

    public int getId() {
        return MultiMapDataSerializerHook.GET_ALL;
    }
}
