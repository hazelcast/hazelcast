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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExplicitClassDefinitionRegistrationTest {

    public static class MyPortable1 implements Portable {

        public static final int ID = 1;

        private String stringField;

        public MyPortable1(String stringField) {
            this.stringField = stringField;
        }

        public MyPortable1() {

        }

        @Override
        public int getFactoryId() {
            return MyPortableFactory1.ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(final PortableWriter writer) throws IOException {
            writer.writeString("stringField", stringField);
        }

        @Override
        public void readPortable(final PortableReader reader) throws IOException {
            stringField = reader.readString("stringField");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyPortable1 that = (MyPortable1) o;

            return stringField != null ? stringField.equals(that.stringField) : that.stringField == null;
        }

        @Override
        public int hashCode() {
            return stringField != null ? stringField.hashCode() : 0;
        }
    }

    public static class MyPortable2 implements Portable {

        public static final int ID = 1;

        private int intField;

        public MyPortable2(int intField) {
            this.intField = intField;
        }

        public MyPortable2() {

        }

        @Override
        public int getFactoryId() {
            return MyPortableFactory2.ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(final PortableWriter writer) throws IOException {
            writer.writeInt("intField", intField);
        }

        @Override
        public void readPortable(final PortableReader reader) throws IOException {
            intField = reader.readInt("intField");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyPortable2 that = (MyPortable2) o;

            return intField == that.intField;
        }

        @Override
        public int hashCode() {
            return intField;
        }
    }

    public static class MyPortableFactory1 implements PortableFactory {

        public static final int ID = 1;

        @Override
        public Portable create(final int classId) {
            if (classId == MyPortable1.ID) {
                return new MyPortable1();
            }
            return null;
        }

    }

    public static class MyPortableFactory2 implements PortableFactory {

        public static final int ID = 2;

        @Override
        public Portable create(final int classId) {
            if (classId == MyPortable2.ID) {
                return new MyPortable2();
            }
            return null;
        }

    }


    @Test
    public void test_classesWithSameClassIdInDifferentFactories() {
        SerializationService ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(MyPortableFactory1.ID, new MyPortableFactory1())
                .addPortableFactory(MyPortableFactory2.ID, new MyPortableFactory2())
                .addClassDefinition(new ClassDefinitionBuilder(MyPortableFactory1.ID, MyPortable1.ID)
                        .addStringField("stringField")
                        .build())
                .addClassDefinition(new ClassDefinitionBuilder(MyPortableFactory2.ID, MyPortable2.ID)
                        .addIntField("intField")
                        .build())
                .build();


        MyPortable1 object = new MyPortable1("test");
        Data data = ss.toData(object);
        assertEquals(object, ss.toObject(data));

        MyPortable2 object2 = new MyPortable2(1);
        Data data2 = ss.toData(object2);
        assertEquals(object2, ss.toObject(data2));

    }

    @Test(expected = HazelcastSerializationException.class)
    public void test_classesWithSameClassId_andSameFactoryId() {
        new DefaultSerializationServiceBuilder()
                .addClassDefinition(new ClassDefinitionBuilder(1, 1)
                        .addIntField("stringField")
                        .build())
                .addClassDefinition(new ClassDefinitionBuilder(1, 1)
                        .addIntField("intField")
                        .build())
                .build();
    }
}
