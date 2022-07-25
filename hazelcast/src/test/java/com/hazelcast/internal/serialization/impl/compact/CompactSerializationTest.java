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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSerializationTest {

    private final SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Test
    public void testOverridingDefaultSerializers() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .register(Integer.class, "int", new IntegerSerializer());

        assertThatThrownBy(() -> createSerializationService(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowOverrideDefaultSerializers");
    }

    @Test
    public void testOverridingDefaultSerializers_withAllowOverrideDefaultSerializers() {
        SerializationConfig config = new SerializationConfig();
        config.setAllowOverrideDefaultSerializers(true);
        config.getCompactSerializationConfig()
                .register(Integer.class, "int", new IntegerSerializer());

        SerializationService service = createSerializationService(config);
        Data data = service.toData(42);

        assertTrue(data.isCompact());

        int i = service.toObject(data);

        assertEquals(42, i);
    }

    @Test
    public void testSerializer_withDuplicateFieldNames() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .register(Foo.class, "foo", new DuplicateWritingSerializer());

        SerializationService service = createSerializationService(config);

        Foo foo = new Foo(42);

        assertThatThrownBy(() -> service.toData(foo))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Failed to serialize")
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasRootCauseMessage("Field with the name 'bar' already exists");
    }

    @Test
    public void testReadWhenFieldDoesNotExist() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .register(Foo.class, "foo", new NonExistingFieldReadingSerializer());

        SerializationService service = createSerializationService(config);

        Foo foo = new Foo(42);
        Data data = service.toData(foo);

        assertThatThrownBy(() -> service.toObject(data))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Unknown field name");
    }

    private SerializationService createSerializationService(SerializationConfig config) {
        config.getCompactSerializationConfig().setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(config)
                .build();
    }

    private static class IntegerSerializer implements CompactSerializer<Integer> {
        @Nonnull
        @Override
        public Integer read(@Nonnull CompactReader in) {
            return in.readInt32("field");
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull Integer object) {
            out.writeInt32("field", object);
        }
    }

    private static class Foo {
        private final int bar;

        private Foo(int bar) {
            this.bar = bar;
        }
    }

    private static class DuplicateWritingSerializer implements CompactSerializer<Foo> {
        @Nonnull
        @Override
        public Foo read(@Nonnull CompactReader in) {
            return new Foo(in.readInt32("bar"));
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull Foo object) {
            out.writeInt32("bar", object.bar);
            out.writeInt32("bar", object.bar);
        }
    }

    private static class NonExistingFieldReadingSerializer implements CompactSerializer<Foo> {
        @Nonnull
        @Override
        public Foo read(@Nonnull CompactReader in) {
            return new Foo(in.readInt32("nonExistingField"));
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull Foo object) {
            out.writeInt32("bar", object.bar);
        }
    }
}
