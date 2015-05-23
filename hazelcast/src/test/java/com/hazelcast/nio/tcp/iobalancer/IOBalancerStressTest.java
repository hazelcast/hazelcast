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

package com.hazelcast.nio.tcp.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerStressTest extends HazelcastTestSupport {
    private static final int TEST_DURATION_SECONDS = 30;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testEachConnectionUseDifferentSelectorEventually() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_IO_BALANCER_INTERVAL_SECONDS, "1");
        config.setProperty(GroupProperties.PROP_IO_THREAD_COUNT, "2");

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        instance2.shutdown();
        instance2 = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(randomMapName());

        long deadLine = System.currentTimeMillis() + TEST_DURATION_SECONDS * 1000;
        for (int i = 0; System.currentTimeMillis() < deadLine; i++) {
            map.put(i % 1000, i);
        }

        TcpIpConnectionManager manager1 = (TcpIpConnectionManager) getConnectionManager(instance1);
        Address address1 = getAddress(instance1);

        TcpIpConnectionManager manager2 = (TcpIpConnectionManager) getConnectionManager(instance2);
        Address address2 = getAddress(instance2);

        TcpIpConnectionManager manager3 = (TcpIpConnectionManager) getConnectionManager(instance3);
        Address address3 = getAddress(instance3);

        assertUseDifferentSelectors(manager1, address2, address3);
        assertUseDifferentSelectors(manager2, address1, address3);
        assertUseDifferentSelectors(manager3, address1, address2);

    }

    private void assertUseDifferentSelectors(TcpIpConnectionManager manager, Address address1, Address address2) {
        TcpIpConnection connection1 = (TcpIpConnection) manager.getConnection(address1);
        TcpIpConnection connection2 = (TcpIpConnection) manager.getConnection(address2);

        assertReadHandlersHaveDifferentOwners(connection1, connection2);
        assertWriteHandlersHaveDifferentOwners(connection1, connection2);
    }

    private void assertWriteHandlersHaveDifferentOwners(TcpIpConnection connection1, TcpIpConnection connection2) {
        WriteHandler writeHandler1 = connection1.getWriteHandler();
        WriteHandler writeHandler2 = connection2.getWriteHandler();
        assertHaveDifferentOwners(writeHandler1, writeHandler2);
    }

    private void assertReadHandlersHaveDifferentOwners(TcpIpConnection connection1, TcpIpConnection connection2) {
        ReadHandler readHandler1 = connection1.getReadHandler();
        ReadHandler readHandler2 = connection2.getReadHandler();
        assertHaveDifferentOwners(readHandler1, readHandler2);
    }

    private void assertHaveDifferentOwners(MigratableHandler handler1, MigratableHandler handler2) {
        IOSelector owner1 = handler1.getOwner();
        IOSelector owner2 = handler2.getOwner();

        assertNotEquals(owner1, owner2);
    }


}