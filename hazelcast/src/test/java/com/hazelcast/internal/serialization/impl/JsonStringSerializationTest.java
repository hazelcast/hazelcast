/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.JsonString;
import com.hazelcast.core.JsonStringImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JsonStringSerializationTest {

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testSerializaeDeserializeJsonString() {
        JsonString json = new JsonStringImpl("{ key: value }");
        Data jsonData = serializationService.toData(json);
        JsonString jsonDeserialized = serializationService.toObject(jsonData);
        assertEquals(json, jsonDeserialized);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidJsonStringFailsToDeserialize() {
        JsonString jsonString = new JsonStringImpl("{");
        Data jsonData = serializationService.toData(jsonString);
        JsonString jsonDeserialized = serializationService.toObject(jsonData);

        jsonDeserialized.asJsonValue();
    }
}
