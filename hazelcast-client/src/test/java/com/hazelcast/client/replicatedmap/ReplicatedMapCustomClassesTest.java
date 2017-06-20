/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapCustomClassesTest extends HazelcastTestSupport {

    @Parameters(name = "replicatedMapFormat:{0} withEqualsAndHashCode:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},

                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean withEqualsAndHashCode;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private ReplicatedMap<CustomKey, CustomValue> map;

    @Before
    public void setUp() {
        Config config = getConfig();
        config.getReplicatedMapConfig("default")
                .setInMemoryFormat(inMemoryFormat);
        prepareSerializationConfig(config.getSerializationConfig());

        ClientConfig clientConfig = new ClientConfig();
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        hazelcastFactory.newHazelcastInstance(config);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        map = client.getReplicatedMap("default");
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void whenCustomClassKeyIsUsed_thenPutAndGetShouldWorkProperly() {
        CustomKey key = new CustomKey(withEqualsAndHashCode);
        CustomValue value = new CustomValue(withEqualsAndHashCode);

        map.put(key, value);
        CustomValue result = map.get(key);

        assertNotNull("The result from map.get(key) should never be null", result);
        if (withEqualsAndHashCode) {
            assertEquals(value, result);
        } else {
            assertEquals(value.getClass(), result.getClass());
            assertFalse(result.executeEqualsAndHashCode);
        }
    }

    private static void prepareSerializationConfig(SerializationConfig serializationConfig) {
        ClassDefinition keyClassDefinition = new ClassDefinitionBuilder(CustomKey.FACTORY_ID, CustomKey.CLASS_ID)
                .addBooleanField("b").build();
        serializationConfig.addClassDefinition(keyClassDefinition);

        ClassDefinition valueClassDefinition = new ClassDefinitionBuilder(CustomValue.FACTORY_ID, CustomValue.CLASS_ID)
                .addBooleanField("b").build();
        serializationConfig.addClassDefinition(valueClassDefinition);

        serializationConfig.addPortableFactory(CustomKey.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomKey();
            }
        });
        serializationConfig.addPortableFactory(CustomValue.FACTORY_ID, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomValue();
            }
        });
    }

    private static class CustomKey extends CustomClass {

        private static int FACTORY_ID = 1;
        private static int CLASS_ID = 1;

        CustomKey() {
        }

        CustomKey(boolean executeEqualsAndHashCode) {
            super(executeEqualsAndHashCode);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }
    }

    private static class CustomValue extends CustomClass {

        private static int FACTORY_ID = 2;
        private static int CLASS_ID = 2;

        CustomValue() {
        }

        CustomValue(boolean executeEqualsAndHashCode) {
            super(executeEqualsAndHashCode);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }
    }

    private static abstract class CustomClass implements Portable {

        boolean executeEqualsAndHashCode;

        CustomClass() {
        }

        CustomClass(boolean executeEqualsAndHashCode) {
            this.executeEqualsAndHashCode = executeEqualsAndHashCode;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeBoolean("b", executeEqualsAndHashCode);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            executeEqualsAndHashCode = reader.readBoolean("b");
        }

        @Override
        public boolean equals(Object o) {
            if (!executeEqualsAndHashCode) {
                return super.equals(o);
            }
            if (this == o) {
                return true;
            }
            if (!(o instanceof CustomClass)) {
                return false;
            }

            CustomClass that = (CustomClass) o;
            return that.executeEqualsAndHashCode;
        }

        @Override
        public int hashCode() {
            if (!executeEqualsAndHashCode) {
                return super.hashCode();
            }
            return 1;
        }
    }
}
