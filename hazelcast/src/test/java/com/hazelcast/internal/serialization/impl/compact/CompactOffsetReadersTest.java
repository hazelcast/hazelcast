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
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static org.junit.Assert.assertEquals;

class OffsetReaderTestDTO {
    public String[] arrayOfStr;
    public int i;
    public String str;

    public OffsetReaderTestDTO(String[] arrayOfStr, int i, String str) {
        this.arrayOfStr = arrayOfStr;
        this.i = i;
        this.str = str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetReaderTestDTO a = (OffsetReaderTestDTO) o;
        return i == a.i && Arrays.equals(arrayOfStr, a.arrayOfStr) && Objects.equals(str, a.str);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(i, str);
        result = 31 * result + Arrays.hashCode(arrayOfStr);
        return result;
    }
}

class OffsetReaderTestDTOSerializer implements CompactSerializer<OffsetReaderTestDTO> {
    @Nonnull
    @Override
    public OffsetReaderTestDTO read(@Nonnull CompactReader in) {
        String[] arrayOfStr = in.readArrayOfString("arrayOfStr");
        int i = in.readInt32("i");
        String str = in.readString("str");
        return new OffsetReaderTestDTO(arrayOfStr, i, str);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull OffsetReaderTestDTO object) {
        out.writeArrayOfString("arrayOfStr", object.arrayOfStr);
        out.writeInt32("i", object.i);
        out.writeString("str", object.str);
    }
}

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class CompactOffsetReadersTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    private static String[] createStringArray(int itemCount) {
        String[] arr = new String[itemCount + 1];
        for (int i = 1; i <= itemCount; i++) {
            // create a string made up of n copies of string s
            arr[i] = String.join("", Collections.nCopies(i * 100, "x"));
        }
        arr[itemCount] = null;
        return arr;
    }

    @Parameterized.Parameter
    public int itemCount;

    @Parameterized.Parameters(name = "itemCount:{0}")
    public static Object[] parameters() {
        return new Object[]{1, 20, 42};
    }

    @Test
    public void testObjectWithDifferentPositionReadersWithCustomSerializer() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.register(OffsetReaderTestDTO.class, "a", new OffsetReaderTestDTOSerializer());
        compactSerializationConfig.setEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
        String[] strArray = createStringArray(itemCount);
        OffsetReaderTestDTO expected = new OffsetReaderTestDTO(strArray, 32, "hey");

        Data data = serializationService.toData(expected);
        OffsetReaderTestDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testObjectWithDifferentPositionReadersWithReflectiveSerializer() {
        SerializationService serializationService = createSerializationService(schemaService);
        String[] strArray = createStringArray(itemCount);
        OffsetReaderTestDTO expected = new OffsetReaderTestDTO(strArray, 32, "hey");

        Data data = serializationService.toData(expected);
        OffsetReaderTestDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }
}
