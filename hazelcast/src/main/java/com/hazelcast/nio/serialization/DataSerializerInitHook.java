/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.map.*;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializer.DataSerializableFactory;
import com.hazelcast.spi.impl.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 8/24/12
 */
final class DataSerializerInitHook {

    static Map<String, DataSerializableFactory> createFactories() {
        final Map<String, DataSerializableFactory> factories = new HashMap<String, DataSerializableFactory>(100);

        factories.put(PutOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new PutOperation();
            }
        });
        factories.put(Response.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new Response();
            }
        });
        factories.put(UpdateResponse.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new UpdateResponse();
            }
        });
        factories.put(AsyncBackupResponse.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new AsyncBackupResponse();
            }
        });
        factories.put(GenericBackupOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new GenericBackupOperation();
            }
        });
        factories.put(GetOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new GetOperation();
            }
        });

        return factories;
    }
}
