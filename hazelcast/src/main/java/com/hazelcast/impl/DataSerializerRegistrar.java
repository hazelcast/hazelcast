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

package com.hazelcast.impl;

import com.hazelcast.impl.map.*;
import com.hazelcast.impl.spi.Response;
import com.hazelcast.nio.DataSerializable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 8/24/12
 */
public final class DataSerializerRegistrar {

    private static final Map<String, DataSerializableFactory> FACTORIES;

    static {
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
        FACTORIES = Collections.unmodifiableMap(factories);
    }

    public static DataSerializableFactory getFactory(String name) {
        return FACTORIES.get(name);
    }

    public interface DataSerializableFactory {
        DataSerializable create();
    }
}
