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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SerializersHazelcastInstanceAwareTest extends HazelcastTestSupport {

    @Test
    public void testPortableFactoryInstance() {
        HazelcastInstanceAwarePortableFactory factory = new HazelcastInstanceAwarePortableFactory();

        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(1, factory);

        HazelcastInstance instance = createHazelcastInstance(config);
        Map<String, PortablePerson> map = instance.getMap("map");

        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set", person.hz);
    }

    @Test
    public void testPortableFactoryClass() {
        Config config = new Config();
        config.getSerializationConfig()
                .addPortableFactoryClass(1, HazelcastInstanceAwarePortableFactory.class.getName());

        HazelcastInstance instance = createHazelcastInstance(config);
        Map<String, PortablePerson> map = instance.getMap("map");
        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set", person.hz);
    }

    @Test
    public void testDataSerializableFactoryInstance() {
        HazelcastInstanceAwareDataSerializableFactory factory = new HazelcastInstanceAwareDataSerializableFactory();

        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, factory);

        HazelcastInstance instance = createHazelcastInstance(config);
        Map<String, DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set", person.hz);
    }

    @Test
    public void testDataSerializableFactoryClass() {
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactoryClass(1,
                HazelcastInstanceAwareDataSerializableFactory.class.getName());

        HazelcastInstance instance = createHazelcastInstance(config);
        Map<String, DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set", person.hz);
    }

    private static class HazelcastInstanceAwarePortableFactory implements PortableFactory, HazelcastInstanceAware {

        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Portable create(int classId) {
            PortablePerson p = new PortablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class PortablePerson implements Portable {
        private HazelcastInstance hz;

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
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
        }
    }

    private static class HazelcastInstanceAwareDataSerializableFactory
            implements DataSerializableFactory, HazelcastInstanceAware {

        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            DataSerializablePerson p = new DataSerializablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class DataSerializablePerson implements IdentifiedDataSerializable {

        private HazelcastInstance hz;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
