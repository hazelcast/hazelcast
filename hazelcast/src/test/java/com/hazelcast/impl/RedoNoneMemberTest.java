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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class RedoNoneMemberTest extends RedoTestBase {

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
    public void testMultiCallToNoneMember() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        RedoTestBase.BeforeAfterTester t = new BeforeAfterTester(new NoneMemberBehavior(h1), new RedoTestBase.MultiCallBuilder(h1));
        t.run();
    }

    @Test(timeout = 100000)
    public void testMapRemoteTargetNotMember() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final Node node1 = getNode(h1);
        CallBuilder callBuilder = new KeyCallBuilder(h1);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            @Override
            void before() throws Exception {
                migrateKey(1, h1, h2);
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node1.clusterManager.removeMember((MemberImpl) h2.getCluster().getLocalMember());
                        assertEquals(1, node1.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            @Override
            void after() {
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        MemberImpl secondMember = new MemberImpl(((MemberImpl) h2.getCluster().getLocalMember()).getAddress(), false);
                        node1.clusterManager.addMember(secondMember);
                        assertEquals(2, node1.clusterManager.lsMembers.size());
                    }
                }, 5);
            }
        };
        new BeforeAfterTester(behavior, callBuilder).run();
    }

    @Test(timeout = 100000)
    public void testMapCallerNotMember() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final Node node2 = getNode(h2);
        CallBuilder callBuilder = new KeyCallBuilder(h1);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            @Override
            void before() throws Exception {
                migrateKey(1, h1, h2);
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node2.clusterManager.removeMember((MemberImpl) h1.getCluster().getLocalMember());
                        assertEquals(1, node2.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            @Override
            void after() {
                final Node node2 = getNode(h2);
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        MemberImpl secondMember = new MemberImpl(((MemberImpl) h1.getCluster().getLocalMember()).getAddress(), false);
                        node2.clusterManager.addMember(secondMember);
                        assertEquals(2, node2.clusterManager.lsMembers.size());
                    }
                }, 5);
            }
        };
        new BeforeAfterTester(behavior, callBuilder).run();
    }

    @Test(timeout = 100000)
    public void testQueueRemoteCallerNoneMember() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        CallBuilder calls = new QueueCallBuilder(h2);
        final Node node1 = getNode(h1);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            public void before() throws Exception {
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node1.clusterManager.removeMember((MemberImpl) h2.getCluster().getLocalMember());
                        assertEquals(1, node1.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            public void after() {
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        MemberImpl secondMember = new MemberImpl(((MemberImpl) h2.getCluster().getLocalMember()).getAddress(), false);
                        node1.clusterManager.addMember(secondMember);
                        assertEquals(2, node1.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            public void destroy() {
            }
        };
        BeforeAfterTester t = new BeforeAfterTester(behavior, calls);
        t.run();
    }

    @Test(timeout = 100000)
    public void testQueueRemoteTargetNoneMember() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        CallBuilder calls = new QueueCallBuilder(h2);
        final Node node2 = getNode(h2);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            public void before() throws Exception {
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node2.clusterManager.removeMember((MemberImpl) h1.getCluster().getLocalMember());
                        assertEquals(1, node2.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            public void after() {
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        MemberImpl secondMember = new MemberImpl(((MemberImpl) h1.getCluster().getLocalMember()).getAddress(), false);
                        node2.clusterManager.addMember(secondMember);
                        assertEquals(2, node2.clusterManager.lsMembers.size());
                    }
                }, 5);
            }

            public void destroy() {
            }
        };
        BeforeAfterTester t = new BeforeAfterTester(behavior, calls);
        t.run();
    }

    @Ignore
    class NoneMemberBehavior extends BeforeAfterBehavior {
        final HazelcastInstance h;
        final Node node;

        NoneMemberBehavior(HazelcastInstance h) {
            this.h = h;
            this.node = getNode(h);
        }

        @Override
        void before() throws Exception {
            List<MemberImpl> lsNew = new LinkedList<MemberImpl>();
            lsNew.add((MemberImpl) h.getCluster().getLocalMember());
            lsNew.add(new MemberImpl(new Address("127.0.0.1", 5702), false));
            node.getClusterImpl().setMembers(lsNew);
        }

        @Override
        void after() {
            List<MemberImpl> lsNew = new LinkedList<MemberImpl>();
            lsNew.add((MemberImpl) h.getCluster().getLocalMember());
            node.getClusterImpl().setMembers(lsNew);
        }
    }
}
