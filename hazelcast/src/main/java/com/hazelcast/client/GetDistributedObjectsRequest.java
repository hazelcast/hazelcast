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

package com.hazelcast.client;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;

import java.util.ArrayList;
import java.util.Collection;

/**
 * User: sancar
 * Date: 8/16/13
 * Time: 10:35 AM
 */
public class GetDistributedObjectsRequest extends ClientRequest implements Portable {
    @Override
    void process() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final Collection<DistributedObject> distributedObjects = clientEngine.getProxyService().getAllDistributedObjects();
        final SerializationService serializationService = getClientEngine().getSerializationService();
        final ArrayList<Data> dataArrayList = new ArrayList<Data>(distributedObjects.size());
        for (DistributedObject distributedObject : distributedObjects) {
            final DistributedObjectInfo distributedObjectInfo = new DistributedObjectInfo(distributedObject.getServiceName(), distributedObject.getName());
            dataArrayList.add(serializationService.toData(distributedObjectInfo));
        }
        SerializableCollection collection = new SerializableCollection(dataArrayList);
        endpoint.sendResponse(collection, getCallId());
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    public int getClassId() {
        return ClientPortableHook.GET_DISTRIBUTED_OBJECT_INFO;
    }

}
