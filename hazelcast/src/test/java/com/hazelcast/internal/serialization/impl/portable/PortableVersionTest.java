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
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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
public class PortableVersionTest {

    // Test for issue https://github.com/hazelcast/hazelcast/issues/12733
    @Test
    public void test_nestedPortable_versionedSerializer() {
        SerializationServiceBuilder builder1 = new DefaultSerializationServiceBuilder();
        builder1.setPortableVersion(6);
        builder1.addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 1) {
                    return new Parent();
                } else if (classId == 2) {
                    return new Child();
                }
                return null;
            }
        });
        SerializationService ss1 = builder1.build();

        SerializationServiceBuilder builder2 = new DefaultSerializationServiceBuilder();
        builder2.setPortableVersion(6);
        builder2.addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 1) {
                    return new Parent();
                } else if (classId == 2) {
                    return new Child();
                }
                return null;
            }
        });
        SerializationService ss2 = builder2.build();

        //make sure ss2 cached class definition of Child
        ss2.toData(new Child("sancar"));

        //serialized parent from ss1
        Parent parent = new Parent(new Child("sancar"));
        Data data = ss1.toData(parent);

        // cached class definition of Child and the class definition from data coming from ss1 should be compatible
        assertEquals(parent, ss2.toObject(data));
    }

    private static class Child implements Portable {

        private String name;

        Child() {
        }

        Child(String name) {
            this.name = name;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("name", name);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Child child = (Child) o;
            return name != null ? name.equals(child.name) : child.name == null;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    private static class Parent implements Portable {

        private Child child;

        Parent() {
        }

        Parent(Child child) {
            this.child = child;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("child", child);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            child = reader.readPortable("child");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Parent parent = (Parent) o;
            return child != null ? child.equals(parent.child) : parent.child == null;
        }

        @Override
        public int hashCode() {
            return child != null ? child.hashCode() : 0;
        }
    }
}
