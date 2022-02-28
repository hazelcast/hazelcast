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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CustomSerializationTest {

    @Test
    public void testSerializer() {
        testSerializer(ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testSerializerLittleEndian() {
        testSerializer(ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testSerializerNativeOrder() {
        testSerializer(ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testSerializerNativeOrderUsingUnsafe() {
        testSerializer(ByteOrder.nativeOrder(), true);
    }

    private void testSerializer(ByteOrder order, boolean allowUnsafe) {
        SerializationConfig config = new SerializationConfig();
        config.setAllowUnsafe(allowUnsafe).setByteOrder(order).setUseNativeByteOrder(false);
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new FooXmlSerializer())
                .setTypeClass(Foo.class);
        config.addSerializerConfig(sc);
        SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();
        Foo foo = new Foo("f");
        Data d = ss.toData(foo);
        Foo newFoo = ss.toObject(d);
        assertEquals(newFoo, foo);
    }

    public static class Foo {

        private String foo;

        public Foo() {
        }

        public Foo(String foo) {
            this.foo = foo;
        }

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Foo foo1 = (Foo) o;

            return !(foo != null ? !foo.equals(foo1.foo) : foo1.foo != null);

        }

        @Override
        public int hashCode() {
            return foo != null ? foo.hashCode() : 0;
        }
    }

    @SuppressWarnings("unused")
    public static class FooDataSerializable extends Foo implements DataSerializable {

        AtomicInteger serializationCount = new AtomicInteger();
        private String foo;

        public FooDataSerializable() {
        }

        public FooDataSerializable(String foo) {
            this.foo = foo;
        }

        @Override
        public String getFoo() {
            return foo;
        }

        @Override
        public void setFoo(String foo) {
            this.foo = foo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Foo that = (Foo) o;
            return !(foo != null ? !foo.equals(that.foo) : that.foo != null);
        }

        @Override
        public int hashCode() {
            return foo != null ? foo.hashCode() : 0;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationCount.incrementAndGet();
            out.writeString(foo);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            foo = in.readString();
        }

        @Override
        public String toString() {
            return "FooDataSerializable{"
                    + "foo='" + foo + '\''
                    + '}';
        }
    }

    public static class FooXmlSerializer implements StreamSerializer<Foo> {

        AtomicInteger serializationCount = new AtomicInteger();

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public void write(ObjectDataOutput out, Foo object) throws IOException {
            serializationCount.incrementAndGet();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            XMLEncoder encoder = new XMLEncoder(bos);
            encoder.writeObject(object);
            encoder.close();
            out.write(bos.toByteArray());
        }

        @Override
        public Foo read(ObjectDataInput in) {
            final InputStream inputStream = (InputStream) in;
            XMLDecoder decoder = new XMLDecoder(inputStream);
            return (Foo) decoder.readObject();
        }

        @Override
        public void destroy() {
        }
    }
}
