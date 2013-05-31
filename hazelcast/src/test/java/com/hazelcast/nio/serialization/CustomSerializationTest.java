/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.TypeSerializerConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class CustomSerializationTest {

    @Test
    public void testTypeSerializer() throws Exception {
        SerializationConfig config = new SerializationConfig();
        TypeSerializerConfig tsc = new TypeSerializerConfig().
                setImplementation(new FooXmlSerializer()).
                setTypeClass(Foo.class);
        config.addTypeSerializer(tsc);
        SerializationService ss = new SerializationServiceImpl(config, null);
        Foo foo = new Foo();
        foo.setFoo("f");
        Data d = ss.toData(foo);
        Foo newFoo = (Foo) ss.toObject(d);
        assertEquals(newFoo.getFoo(), foo.getFoo());
    }

    public static class Foo {

        private String foo;

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }
    }

    public static class FooXmlSerializer implements TypeSerializer<Foo> {

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public void write(ObjectDataOutput out, Foo object) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            XMLEncoder encoder = new XMLEncoder(bos);
            encoder.writeObject(object);
            encoder.close();
            out.write(bos.toByteArray());
        }

        @Override
        public Foo read(ObjectDataInput in) throws IOException {
            final InputStream inputStream = (InputStream) in;
            XMLDecoder decoder = new XMLDecoder(inputStream);
            return (Foo) decoder.readObject();
        }

        @Override
        public void destroy() {
        }
    }
}
