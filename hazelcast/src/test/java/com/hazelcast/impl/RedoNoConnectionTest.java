/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class RedoNoConnectionTest extends RedoTestBase {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 100000)
    public void testMultiCallToNotConnectedMember() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        BeforeAfterTester t = new BeforeAfterTester(
                new NoConnectionBehavior(h1, h2),
                new MultiCallBuilder(h1));
        t.run();
    }

    @Test(timeout = 100000)
    public void testKeyBasedCallToNotConnectedMember() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        migrateKey(1, h1, h2);
        BeforeAfterTester t = new BeforeAfterTester(
                new NoConnectionBehavior(h1, h2),
                new KeyCallBuilder(h1));
        t.run();
    }

    @Test(timeout = 100000)
    public void testQueueCallToNotConnectedMember() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        BeforeAfterTester t = new BeforeAfterTester(
                new NoConnectionBehavior(h2, h1),
                new QueueCallBuilder(h2));
        t.run();
    }

    @Test(timeout = 100000)
    public void testQueueCallToDisconnectingMember() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        BeforeAfterTester t = new BeforeAfterTester(
                new DisconnectionBehavior(h2, h1),
                new QueueCallBuilder(h2));
        t.run();
    }

    @Ignore
    class NoConnectionBehavior extends BeforeAfterBehavior {
        final HazelcastInstance caller;
        final HazelcastInstance target;
        final Connection connTarget;
        final Node callerNode;

        NoConnectionBehavior(HazelcastInstance caller, HazelcastInstance target) {
            this.caller = caller;
            this.target = target;
            this.callerNode = getNode(caller);
            Address targetAddress = ((MemberImpl) target.getCluster().getLocalMember()).getAddress();
            connTarget = callerNode.getConnectionManager().getConnection(targetAddress);
        }

        @Override
        void before() throws Exception {
            callerNode.getConnectionManager().detachAndGetConnection(connTarget.getEndPoint());
        }

        @Override
        void after() {
            callerNode.getConnectionManager().attachConnection(connTarget.getEndPoint(), connTarget);
        }
    }

    @Ignore
    class DisconnectionBehavior extends BeforeAfterBehavior {
        final HazelcastInstance caller;
        final HazelcastInstance target;
        final Connection connTarget;
        final Node callerNode;

        DisconnectionBehavior(HazelcastInstance caller, HazelcastInstance target) {
            this.caller = caller;
            this.target = target;
            this.callerNode = getNode(caller);
            Address targetAddress = ((MemberImpl) target.getCluster().getLocalMember()).getAddress();
            connTarget = callerNode.getConnectionManager().getConnection(targetAddress);
        }

        @Override
        void before() throws Exception {
            callerNode.getConnectionManager().detachAndGetConnection(connTarget.getEndPoint());
        }

        @Override
        void after() {
            callerNode.getConnectionManager().destroyConnection(connTarget);
        }
    }
}
