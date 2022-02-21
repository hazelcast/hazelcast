/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.json.internal.JsonSchemaHelper;
import com.hazelcast.internal.serialization.Data;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.HeapData.HEAP_DATA_OVERHEAD;

public class JsonMetadataInitializer implements MetadataInitializer {

    public static final JsonMetadataInitializer INSTANCE = new JsonMetadataInitializer();

    private static final int UTF_CHAR_COUNT_FIELD_SIZE = 4;

    private static final JsonFactory FACTORY = new JsonFactory();

    public Object createFromData(Data data) throws IOException {
        if (data.isJson()) {
            try (JsonParser parser = FACTORY.createParser(new ByteArrayInputStream(data.toByteArray(),
                    HEAP_DATA_OVERHEAD + UTF_CHAR_COUNT_FIELD_SIZE, data.dataSize() - UTF_CHAR_COUNT_FIELD_SIZE))) {
                return JsonSchemaHelper.createSchema(parser);
            }
        }
        return null;
    }

    public Object createFromObject(Object obj) throws IOException {
        if (obj instanceof HazelcastJsonValue) {
            String str = obj.toString();
            try (JsonParser parser = FACTORY.createParser(str)) {
                return JsonSchemaHelper.createSchema(parser);
            }
        }
        return null;
    }
}
