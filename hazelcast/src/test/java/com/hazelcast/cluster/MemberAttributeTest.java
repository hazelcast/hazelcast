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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class MemberAttributeTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testPresharedAttributes() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", Integer.valueOf(123));
        
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());
        
        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
        	if (m == h2.getCluster().getLocalMember())
        		continue;
        	member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
    	assertNotNull(member.getAttribute("Test"));
    	assertEquals(123, member.getAttribute("Test"));
        
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testAddAttributes() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", Integer.valueOf(123));
        
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());
        
        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
        	if (m == h2.getCluster().getLocalMember())
        		continue;
        	member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
    	assertNotNull(member.getAttribute("Test"));
    	assertEquals(123, member.getAttribute("Test"));
        
        m1.setAttribute("Test2", Integer.valueOf(321));
        
        // Force sleep to distribute value
        sleep();
    
        assertNotNull(member.getAttribute("Test2"));
    	assertEquals(321, member.getAttribute("Test2"));
        
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void testChangeAttributes() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", Integer.valueOf(123));
        
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());
        
        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
        	if (m == h2.getCluster().getLocalMember())
        		continue;
        	member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
    	assertNotNull(member.getAttribute("Test"));
    	assertEquals(123, member.getAttribute("Test"));
        
        m1.setAttribute("Test", Integer.valueOf(321));
        
        // Force sleep to distribute value
        sleep();
        
        assertNotNull(member.getAttribute("Test"));
    	assertEquals(321, member.getAttribute("Test"));
        
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }
    @Test(timeout = 120000)
    public void testRemoveAttributes() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        m1.setAttribute("Test", Integer.valueOf(123));
        
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h2.getCluster().getMembers().size());
        
        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
        	if (m == h2.getCluster().getLocalMember())
        		continue;
        	member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
    	assertNotNull(member.getAttribute("Test"));
    	assertEquals(123, member.getAttribute("Test"));
        
        m1.setAttribute("Test", null);
        
        // Force sleep to distribute value
        sleep();
        
        assertNull(member.getAttribute("Test"));
        
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    private void sleep() {
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
    }
    
}
