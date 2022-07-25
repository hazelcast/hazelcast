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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class CompactBooleanFieldTest {
    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Parameterized.Parameter
    public int itemCount;


    @Parameterized.Parameters(name = "itemCount:{0}")
    public static Object[] parameters() {
        return new Object[]{0, 1, 8, 10, 100, 1000};
    }

    private static boolean[] getBooleans(int count) {
        boolean[] booleans = new boolean[count];
        Random r = new Random();
        for (int i = 0; i < count; i++) {
            booleans[i] = r.nextInt(10) % 2 == 0;
        }
        return booleans;
    }

    @Test
    public void testBooleanArrayWithCustomSerializer() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.register(BoolArrayDTO.class, "bools", new BoolArrayDTOSerializer());
        compactSerializationConfig.setEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
        boolean[] bools = getBooleans(itemCount);
        BoolArrayDTO expected = new BoolArrayDTO(bools);

        Data data = serializationService.toData(expected);
        BoolArrayDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testBooleanArrayWithReflectiveSerializer() {
        SerializationService serializationService = createSerializationService(schemaService);
        boolean[] bools = getBooleans(itemCount);
        BoolArrayDTO expected = new BoolArrayDTO(bools);

        Data data = serializationService.toData(expected);
        BoolArrayDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testMultipleBoolFields() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.register(BoolArrayDTO.class, "bools", new BoolArrayDTOSerializer2(itemCount));
        compactSerializationConfig.setEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
        boolean[] bools = getBooleans(itemCount);
        BoolArrayDTO expected = new BoolArrayDTO(bools);

        Data data = serializationService.toData(expected);
        BoolArrayDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }
}
