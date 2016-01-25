/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {QuickTest.class, ParallelTest.class})
public class ReplicatedMapAntiEntropyTest extends ReplicatedMapBaseTest {

    @After
    public void cleanup() {
        System.clearProperty("hazelcast.serialization.custom.override");
    }

    @Test
    public void testMapConvergesToSameValueWhenMissingReplicationUpdate() throws Exception {
        Config config = new Config();
        SerializationConfig serializationConfig = new SerializationConfig();
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setTypeClassName(PutOperation.class.getName());
        serializerConfig.setImplementation(new PutOperationWithNoReplicationSerializer());
        serializationConfig.addSerializerConfig(serializerConfig);
        config.setSerializationConfig(serializationConfig);
        System.setProperty("hazelcast.serialization.custom.override", "true");
        String mapName = randomMapName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap(mapName);
        final ReplicatedMap<Object, Object> map3 = instance3.getReplicatedMap(mapName);
        final String key = generateKeyOwnedBy(instance2);
        final String value = randomString();
        map1.put(key, value);
        assertEquals(value, map1.get(key));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(value, map2.get(key));
                assertEquals(value, map3.get(key));
            }
        });
    }

    public class PutOperationWithNoReplicationSerializer implements StreamSerializer<PutOperation> {
        @Override
        public void write(ObjectDataOutput out, PutOperation object) throws IOException {
            object.writeData(out);
        }

        @Override
        public PutOperation read(ObjectDataInput in) throws IOException {
            final PutOperationWithNoReplication operation = new PutOperationWithNoReplication();
            operation.readData(in);
            return operation;
        }

        @Override
        public int getTypeId() {
            return 8778;
        }

        @Override
        public void destroy() {

        }
    }

    class PutOperationWithNoReplication extends PutOperation {

        public PutOperationWithNoReplication() {
        }


        @Override
        protected Collection<Address> getMemberAddresses() {
            return Collections.emptyList();
        }
    }

}
