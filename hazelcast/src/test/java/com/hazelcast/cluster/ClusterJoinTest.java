/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
//TODO:
public class ClusterJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testCustomJoiner() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        c.getNetworkConfig().getInterfaces().setEnabled(false);
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c.getNetworkConfig().setPortCount(2);
        c.getNetworkConfig().setPort(6000);
        c.getNetworkConfig().setPortAutoIncrement(true);
        c.getNetworkConfig().getJoin().getCustomConfig().setEnabled(true);
        c.getNetworkConfig().getJoin().getCustomConfig().setJoinerFactoryClassName(CustomJoinerFactory.class.getName());

        c.getNetworkConfig().getJoin().getCustomConfig().getProperties().put("member_1", "127.0.0.1:6000");
        c.getNetworkConfig().getJoin().getCustomConfig().getProperties().put("member_2", "127.0.0.1:6001");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);

        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testTcpIp1() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testTcpIp2() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testTcpIp3() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testMulticast() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testTcpIpWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        try {
            Config c = new Config();
            c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            c.getNetworkConfig().getInterfaces().setEnabled(true);
            c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1:5701");
            c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
            assertEquals(1, h1.getCluster().getMembers().size());
            System.setProperty("hazelcast.build", "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
            assertEquals(2, h1.getCluster().getMembers().size());
            assertEquals(2, h2.getCluster().getMembers().size());
        } finally {
            System.clearProperty("hazelcast.build");
        }
    }

    @Test(timeout = 120000)
    @Category(ProblematicTest.class)
    public void testMulticastWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(1, h1.getCluster().getMembers().size());
            System.setProperty("hazelcast.build", "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(2, h1.getCluster().getMembers().size());
            assertEquals(2, h2.getCluster().getMembers().size());
        } finally {
            System.clearProperty("hazelcast.build");
        }
    }

    /**
     * Test for the issue 184
     * <p/>
     * Hazelcas.newHazelcastInstance(new Config()) doesn't join the cluster.
     * new Config() should be enough as the default config.
     */
    @Test(timeout = 240000)
    @Category(ProblematicTest.class)
    public void testDefaultConfigCluster() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test
    public void unresolvableHostName() {
        Config config = new Config();
        config.getGroupConfig().setName("abc");
        config.getGroupConfig().setPassword("def");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().setMembers(Arrays.asList(new String[]{"localhost", "nonexistinghost"}));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, hz.getCluster().getMembers().size());
    }

    @Test
    public void testNewInstanceByName() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.getHazelcastInstanceByName("test");
        HazelcastInstance hc3 = Hazelcast.getHazelcastInstanceByName(hc1.getName());
        assertTrue(hc1 == hc2);
        assertTrue(hc1 == hc3);
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testMulticastJoinWithIncompatibleGroups() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getGroupConfig().setName("group1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("group2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    public void testTcpIpJoinWithIncompatibleGroups() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config1.getGroupConfig().setName("group1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setName("group2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    public void testMulticastJoinWithIncompatiblePasswords() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getGroupConfig().setPassword("pass1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setPassword("pass2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    public void testTcpIpJoinWithIncompatiblePasswords() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config1.getGroupConfig().setPassword("pass1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getGroupConfig().setPassword("pass2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testJoinWithIncompatibleJoiners() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMulticastJoinWithIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getPartitionGroupConfig().setEnabled(true).setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config2.getPartitionGroupConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    public void testTcpIpJoinWithIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config1.getPartitionGroupConfig().setEnabled(true).setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config2.getPartitionGroupConfig().setEnabled(true).setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }


    @Test
    public void testMulticastJoinDuringSplitBrainHandlerRunning() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        props.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "0");
        props.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "0");

        final CountDownLatch latch = new CountDownLatch(1);
        Config config1 = new Config();
        config1.getNetworkConfig().setPort(5901) ; // bigger port to make sure address.hashCode() check pass during merge!
        config1.setProperties(props);
        config1.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                switch (event.getState()) {
                    case MERGING:
                    case MERGED:
                        latch.countDown();
                    default:
                        break;
                }
            }
        }));
        Hazelcast.newHazelcastInstance(config1);
        Thread.sleep(5000);

        Config config2 = new Config();
        config2.getNetworkConfig().setPort(5701) ;
        config2.setProperties(props);
        Hazelcast.newHazelcastInstance(config2);

        assertFalse("Latch should not be countdown!", latch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Returns an extention of TcpIpJoiner which
     */
    public static class CustomJoinerFactory implements JoinerFactory{

        @Override
        public Joiner createJoiner(final Node node) {
            return new TcpIpJoiner(node){

                @Override
                protected Collection<String> getMembers() {
                    Properties props = node.getConfig().getNetworkConfig().getJoin().getCustomConfig().getProperties();
                    return Arrays.asList(new String[]{(String) props.get("member_1"), (String) props.get("member_2")});
                }

                @Override
                public String getType() {
                    return "custom";
                }

            };
        }
    }
}
