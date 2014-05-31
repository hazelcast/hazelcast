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

package com.hazelcast.instance;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.QuickTest;

/**
 * @author St&amp;eacute;phane Galland <galland@arakhne.org>
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MulticastLoopbackModeTest extends HazelcastTestSupport {

	private String useNetwork;
	private String multicastGroup;
	
	@Before
	public void setUpTests() {
		this.useNetwork = System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, Boolean.TRUE.toString());
        this.multicastGroup = System.clearProperty("hazelcast.multicast.group");
	}
	
	@After
	public void tearDownTests() {
		if (this.useNetwork==null)
			System.clearProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK);
		else
			System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, this.useNetwork);
		if (this.multicastGroup==null)
			System.clearProperty("hazelcast.multicast.group");
		else
			System.setProperty("hazelcast.multicast.group", this.multicastGroup);
	}
	
    @Test
    public void testEnabledMode(){
        Config config1 = new Config();
		{
			config1.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config1.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(true);
		}

        Config config2 = new Config();
		{
			config2.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config2.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(true);
		}

		String dataName = randomMapName(MulticastLoopbackModeTest.class.getName());
		String myID = randomString();
		
        HazelcastInstance hz1 = HazelcastInstanceFactory.newHazelcastInstance(config1);
        assertNotNull("cannot create the first hazelcastInstance",hz1);
        try {
            ISet<String> theMap = hz1.getSet(dataName);
            theMap.add(myID);
            
            HazelcastInstance hz2 = HazelcastInstanceFactory.newHazelcastInstance(config2);
            assertNotNull("cannot create the first hazelcastInstance",hz2);
            try {
                ISet<String> remoteMap = hz2.getSet(dataName);
                
                sleepSeconds(1);
                
                assertTrue(remoteMap.contains(myID));
            }
            finally {
            	if (hz2!=null) TestUtil.terminateInstance(hz2);
            }
        }
        finally {
            if (hz1!=null) TestUtil.terminateInstance(hz1);
        }
    }

    @Test
    public void testDisabledMode(){
        Config config1 = new Config();
		{
			config1.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config1.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(false);
		}

        Config config2 = new Config();
		{
			config2.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config2.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(false);
		}

		String dataName = randomMapName(MulticastLoopbackModeTest.class.getName());
		String myID = randomString();
		
		HazelcastInstance hz1 = HazelcastInstanceFactory.newHazelcastInstance(config1);
        assertNotNull("cannot create the first hazelcastInstance",hz1);
        try {
            ISet<String> theMap = hz1.getSet(dataName);
            theMap.add(myID);
            
            HazelcastInstance hz2 = HazelcastInstanceFactory.newHazelcastInstance(config2);
            assertNotNull("cannot create the first hazelcastInstance",hz2);
            try {
                ISet<String> remoteMap = hz2.getSet(dataName);
                
                sleepSeconds(1);
                
                assertFalse(remoteMap.contains(myID));
            }
            finally {
            	if (hz2!=null) TestUtil.terminateInstance(hz2);
            }
        }
        finally {
        	if (hz1!=null) TestUtil.terminateInstance(hz1);
        }
    }

}
