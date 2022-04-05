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

package com.hazelcast.client.cp.internal;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPSubsystemImplTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().getAtomicLong("long");
    }

    @Test
    public void test_atomicReference_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().getAtomicReference("ref");
    }

    @Test
    public void test_lock_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().getLock("lock");
    }

    @Test
    public void test_semaphore_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().getSemaphore("semaphore");
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().getAtomicLong("latch");
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        thrown.expect(UnsupportedOperationException.class);

        client.getCPSubsystem().getCPSubsystemManagementService();
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemNotConfigured() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        thrown.expect(UnsupportedOperationException.class);

        client.getCPSubsystem().getCPSessionManagementService();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        assertNotNull(client.getCPSubsystem().getAtomicLong("long"));
    }

    @Test
    public void test_atomicReference_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        assertNotNull(client.getCPSubsystem().getAtomicReference("ref"));
    }

    @Test
    public void test_lock_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        assertNotNull(client.getCPSubsystem().getLock("lock"));
    }

    @Test
    public void test_semaphore_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        assertNotNull(client.getCPSubsystem().getSemaphore("semaphore"));
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        assertNotNull(client.getCPSubsystem().getCountDownLatch("latch"));
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        thrown.expect(UnsupportedOperationException.class);

        client.getCPSubsystem().getCPSubsystemManagementService();
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        thrown.expect(UnsupportedOperationException.class);

        client.getCPSubsystem().getCPSessionManagementService();
    }

}
