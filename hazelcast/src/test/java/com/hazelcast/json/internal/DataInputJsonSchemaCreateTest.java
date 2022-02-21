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

package com.hazelcast.json.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.convertToInputStream;

@Category({ParallelJVMTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class DataInputJsonSchemaCreateTest extends AbstractJsonSchemaCreateTest {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private JsonFactory factory = new JsonFactory();

    protected JsonParser createParserFromString(String jsonString) throws IOException {
        return factory.createParser(convertToInputStream(serializationService.createObjectDataInput(serializationService.toBytes(new HazelcastJsonValue(jsonString))), 12));
    }

    @Test
    public void testOneFirstLevelAttribute_withTwoByteCharacterInName() throws IOException {
        String jsonString = Json.object().add("this-name-includes-two-byte-utf8-character-£",
                "so the value should start at next byte location").toString();
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNode description = JsonSchemaHelper.createSchema(parser);

        validate(description, "0", 1, 49);
    }

    @Test
    public void testTwoFirstLevelAttributes_withTwoByteCharacterInValues() throws IOException {
        String jsonString = Json.object()
                .add("anAttribute", "this value has two byte character -£ ")
                .add("secondAttribute", 4).toString();
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNode description = JsonSchemaHelper.createSchema(parser);

        validate(description, "0", 1, 15);
        validate(description, "1", 56, 74);
    }
}
