/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.EmployerDTO;
import example.serialization.HiringStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompactClusterChangeTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    protected HazelcastInstance member1;
    protected HazelcastInstance member2;
    protected HazelcastInstance member3;
    protected HazelcastInstance client;

    @Before
    public void setUp() {
        Config config = getMemberConfig();
        member1 = factory.newHazelcastInstance(config);
        member2 = factory.newHazelcastInstance(config);
        member3 = factory.newHazelcastInstance(config);

        client = getClient();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    protected Config getMemberConfig() {
        return smallInstanceConfig();
    }

    private HazelcastInstance getClient() {
        return factory.newHazelcastClient();
    }

    @Test
    public void testClusterRestart() {
        EmployerDTO employer = newEmployer();
        IMap<Integer, EmployerDTO> map = client.getMap("employer");
        map.put(1, employer);

        changeCluster();

        map.put(1, employer);
        assertEquals(employer, map.get(1));

        // Perform a query to make sure that the schema is available on the cluster
        assertEquals(1, map.values(Predicates.sql("zcode == 42")).size());
    }

    protected void changeCluster() {
        // For OS, changing cluster means restarting it
        member1.shutdown();
        member2.shutdown();
        member3.shutdown();

        Config config = getMemberConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
    }

    private EmployerDTO newEmployer() {
        return new EmployerDTO(
                "foo",
                42,
                HiringStatus.HIRING,
                new long[]{1, 2, 3},
                new EmployeeDTO(42, 24),
                new EmployeeDTO[]{new EmployeeDTO(24, 42), null}
        );
    }
}
