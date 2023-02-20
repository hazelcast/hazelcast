/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactBooleanFieldTest {
    private final SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

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
    public void testBooleanArray() {
        SerializationService serializationService = createSerializationService(BoolArrayDTOSerializer::new);
        boolean[] bools = getBooleans(itemCount);
        BoolArrayDTO expected = new BoolArrayDTO(bools);

        Data data = serializationService.toData(expected);
        BoolArrayDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testMultipleBoolFields() {
        SerializationService serializationService = createSerializationService(() ->
                new BoolArrayDTOAsBooleansSerializer(itemCount));
        boolean[] bools = getBooleans(itemCount);
        BoolArrayDTO expected = new BoolArrayDTO(bools);

        Data data = serializationService.toData(expected);
        BoolArrayDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    private static class BoolArrayDTO {
        private final boolean[] bools;

        BoolArrayDTO(boolean[] bools) {
            this.bools = bools;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BoolArrayDTO b = (BoolArrayDTO) o;
            return Arrays.equals(bools, b.bools);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bools);
        }

        @Override
        public String toString() {
            return "Bools{"
                    + "bools=" + Arrays.toString(bools)
                    + '}';
        }
    }

    private static class BoolArrayDTOSerializer implements CompactSerializer<BoolArrayDTO> {
        @Nonnull
        @Override
        public BoolArrayDTO read(@Nonnull CompactReader in) {
            boolean[] bools = in.readArrayOfBoolean("bools");
            return new BoolArrayDTO(bools);
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull BoolArrayDTO object) {
            out.writeArrayOfBoolean("bools", object.bools);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "bools";
        }

        @Nonnull
        @Override
        public Class<BoolArrayDTO> getCompactClass() {
            return BoolArrayDTO.class;
        }
    }

    private static class BoolArrayDTOAsBooleansSerializer implements CompactSerializer<BoolArrayDTO> {
        private final int itemCount;

        BoolArrayDTOAsBooleansSerializer(int itemCount) {
            this.itemCount = itemCount;
        }

        @Nonnull
        @Override
        public BoolArrayDTO read(@Nonnull CompactReader in) {
            boolean[] bools = new boolean[itemCount];
            for (int i = 0; i < itemCount; i++) {
                bools[i] = in.readBoolean(Integer.toString(i));
            }
            return new BoolArrayDTO(bools);
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull BoolArrayDTO object) {
            for (int i = 0; i < itemCount; i++) {
                out.writeBoolean(Integer.toString(i), object.bools[i]);
            }
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "bools";
        }

        @Nonnull
        @Override
        public Class<BoolArrayDTO> getCompactClass() {
            return BoolArrayDTO.class;
        }
    }
}
