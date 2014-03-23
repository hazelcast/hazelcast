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

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdRepository;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class ClientDataSerializerHook implements DataSerializerHook {

    public static final int ID = FactoryIdRepository.getDSFactoryId(FactoryIdRepository.CLIENT);

    public static final int CLIENT_RESPONSE = 1;

    @Override
    public int getFactoryId() {
        return ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case CLIENT_RESPONSE:
                        return new ClientResponse();
                    default:
                        return null;
                }
            }
        };
    }
}
