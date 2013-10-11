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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

/**
 * @author mdogan 3/11/13
 */
public class ClientPortableFactory implements PortableFactory {

    public Portable create(int classId) {
        switch (classId) {
            case ClientPortableHook.GENERIC_ERROR:
                return new GenericError();

            case ClientPortableHook.AUTH:
                return new AuthenticationRequest();

            case ClientPortableHook.PRINCIPAL:
                return new ClientPrincipal();

            case ClientPortableHook.GET_DISTRIBUTED_OBJECT_INFO:
                return new GetDistributedObjectsRequest();

            case ClientPortableHook.DISTRIBUTED_OBJECT_INFO:
                return new DistributedObjectInfo();
            case ClientPortableHook.CREATE_PROXY:
                return new ClientCreateRequest();
            case ClientPortableHook.DESTROY_PROXY:
                return new ClientDestroyRequest();
            case ClientPortableHook.LISTENER:
                return new DistributedObjectListenerRequest();
        }
        return null;
    }
}
