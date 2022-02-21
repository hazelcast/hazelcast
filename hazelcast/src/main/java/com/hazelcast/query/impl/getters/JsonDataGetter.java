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

package com.hazelcast.query.impl.getters;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DataInputNavigableJsonAdapter;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.HeapData.HEAP_DATA_OVERHEAD;

public final class JsonDataGetter extends AbstractJsonGetter {

    private static final int UTF_CHARACTER_COUNT_FIELD_SIZE = 4;

    private JsonFactory factory = new JsonFactory();

    private InternalSerializationService ss;

    JsonDataGetter(InternalSerializationService ss) {
        super(null);
        this.ss = ss;
    }

    protected JsonParser createParser(Object obj) throws IOException {
        Data data = (Data) obj;
        return factory.createParser(data.toByteArray(),
                HEAP_DATA_OVERHEAD + UTF_CHARACTER_COUNT_FIELD_SIZE,
                data.dataSize() - UTF_CHARACTER_COUNT_FIELD_SIZE);
    }

    @Override
    protected NavigableJsonInputAdapter annotate(Object object) {
        Data data = (Data) object;
        return new DataInputNavigableJsonAdapter(ss.createObjectDataInput(data),
                HEAP_DATA_OVERHEAD + UTF_CHARACTER_COUNT_FIELD_SIZE);
    }
}
