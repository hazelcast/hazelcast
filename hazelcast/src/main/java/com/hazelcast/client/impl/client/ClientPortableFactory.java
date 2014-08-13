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

package com.hazelcast.client.impl.client;

import com.hazelcast.cluster.client.AddMembershipListenerRequest;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.partition.client.GetPartitionsRequest;

/**
 * Factory class for central client request
 */
public class ClientPortableFactory implements PortableFactory {

    @Override
    public Portable create(int classId) {
        Portable portable;
        switch (classId) {
            case ClientPortableHook.GENERIC_ERROR:
                portable = new GenericError();
                break;
            case ClientPortableHook.AUTH:
                portable = new AuthenticationRequest();
                break;
            case ClientPortableHook.PRINCIPAL:
                portable = new ClientPrincipal();
                break;
            case ClientPortableHook.GET_DISTRIBUTED_OBJECT_INFO:
                portable = new GetDistributedObjectsRequest();
                break;
            case ClientPortableHook.DISTRIBUTED_OBJECT_INFO:
                portable = new DistributedObjectInfo();
                break;
            case ClientPortableHook.CREATE_PROXY:
                portable = new ClientCreateRequest();
                break;
            case ClientPortableHook.DESTROY_PROXY:
                portable = new ClientDestroyRequest();
                break;
            case ClientPortableHook.LISTENER:
                portable = new DistributedObjectListenerRequest();
                break;
            case ClientPortableHook.MEMBERSHIP_LISTENER:
                portable = new AddMembershipListenerRequest();
                break;
            case ClientPortableHook.CLIENT_PING:
                portable = new ClientPingRequest();
                break;
            case ClientPortableHook.GET_PARTITIONS:
                portable = new GetPartitionsRequest();
                break;
            case ClientPortableHook.REMOVE_LISTENER:
                portable = new RemoveDistributedObjectListenerRequest();
                break;
            default:
                portable = null;
        }
        return portable;
    }
}
