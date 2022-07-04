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

package com.hazelcast.internal.serialization.impl.compact.integration;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class CompactFormatIntegrationTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean serverDoesNotHaveClasses;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, serverDoesNotHaveClasses:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Override
    protected Config getConfig() {
        Config config = super.smallInstanceConfig();
        config.getMapConfig("test").setInMemoryFormat(inMemoryFormat);
        if (serverDoesNotHaveClasses) {
            List<String> excludes = singletonList("example.serialization");
            FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
            config.setClassLoader(classLoader);
        }
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        return config;
    }

    @Before
    public abstract void setup();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testBasic() {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        map.put(1, employeeDTO);

        IMap<Integer, EmployeeDTO> map2 = instance2.getMap("test");
        assertEquals(employeeDTO, map2.get(1));
    }

    @Test
    public void testBasicQuery() {
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, 102310312);
            map.put(i, employeeDTO);
        }

        IMap<Integer, EmployeeDTO> map2 = instance2.getMap("test");
        int size = map2.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);

    }

    @Test
    public void testJoinedMemberQuery() {
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, 102310312);
            map.put(i, employeeDTO);
        }

        HazelcastInstance newInstance = factory.newHazelcastInstance(getConfig());

        waitClusterForSafeState(newInstance);

        IMap<Integer, EmployeeDTO> map2 = newInstance.getMap("test");
        int size = map2.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);
    }

    @Test
    public void testEntryProcessor() {
        IMap<Integer, Object> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            if (serverDoesNotHaveClasses) {
                GenericRecord record = GenericRecordBuilder.compact("employee").setInt32("age", i)
                        .setInt64("id", 102310312).build();
                map.put(i, record);
            } else {
                EmployeeDTO employeeDTO = new EmployeeDTO(i, 102310312);
                map.put(i, employeeDTO);
            }
        }

        IMap map2 = instance2.getMap("test");
        if (serverDoesNotHaveClasses) {
            map2.executeOnEntries(new GenericIncreaseAgeEntryProcessor());
        } else {
            map2.executeOnEntries(new IncreaseAgeEntryProcessor());
        }

        for (int i = 0; i < 100; i++) {
            if (serverDoesNotHaveClasses) {
                GenericRecord record = (GenericRecord) map2.get(i);
                assertEquals(record.getInt32("age"), 1000 + i);
            } else {
                EmployeeDTO employeeDTO = (EmployeeDTO) map.get(i);
                assertEquals(employeeDTO.getAge(), 1000 + i);

            }
        }
    }

    static class IncreaseAgeEntryProcessor implements EntryProcessor<Integer, EmployeeDTO, Object>, Serializable {
        @Override
        public Object process(Map.Entry<Integer, EmployeeDTO> entry) {
            EmployeeDTO value = entry.getValue();
            value.setAge(value.getAge() + 1000);
            entry.setValue(value);
            return null;
        }
    }

    static class GenericIncreaseAgeEntryProcessor implements EntryProcessor<Integer, GenericRecord, Object>, Serializable {
        @Override
        public Object process(Map.Entry<Integer, GenericRecord> entry) {
            GenericRecord value = entry.getValue();
            GenericRecord newValue = value.cloneWithBuilder()
                    .setInt32("age", value.getInt32("age") + 1000)
                    .build();
            entry.setValue(newValue);
            return null;
        }
    }

    @Test
    public void testClusterRestart() {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        map.put(1, employeeDTO);

        restartCluster();

        map.put(1, employeeDTO);
        assertEquals(employeeDTO, map.get(1));
        // Perform a query to make sure that the schema is available on the cluster
        assertEquals(1, map.values(Predicates.sql("age == 30")).size());
    }

    protected abstract void restartCluster();

}
