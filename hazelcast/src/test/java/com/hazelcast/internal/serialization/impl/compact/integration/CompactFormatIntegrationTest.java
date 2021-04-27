/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
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
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class CompactFormatIntegrationTest extends HazelcastTestSupport {

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Override
    protected Config getConfig() {
        Config config = super.smallInstanceConfig();
        config.getMapConfig("test").setInMemoryFormat(inMemoryFormat);
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
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, 102310312);
            map.put(i, employeeDTO);
        }

        IMap<Integer, EmployeeDTO> map2 = instance2.getMap("test");
        map2.executeOnEntries(new IncreaseAgeEntryProcessor());

        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = map.get(i);
            assertEquals(employeeDTO.getAge(), 1000 + i);
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

    @Test
    public void testClusterRestart() {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        IMap<Integer, EmployeeDTO> map = instance1.getMap("test");
        map.put(1, employeeDTO);

        restartCluster();

        IMap<Integer, EmployeeDTO> map2 = instance2.getMap("test");
        map2.put(1, employeeDTO);
        assertEquals(employeeDTO, map2.get(1));
    }

    protected abstract void restartCluster();

    @Test
    public void testHotRestart() {
        throw new RuntimeException();
    }

    @Test
    public void testWanReplication() {
        throw new RuntimeException();
    }

}
