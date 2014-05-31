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
import static org.junit.Assume.assumeFalse;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.QuickTest;

/** Test the multicast loopback mode when there is no other
 * network interface than 127.0.0.1.
 *  
 * @author St&amp;eacute;phane Galland <galland@arakhne.org>
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MulticastLoopbackModeTest extends HazelcastTestSupport {

	/** Replies if a network interface was properly configured.
	 * 
	 * @return <code>true</code> if there is at least one configured interface;
	 * <code>false</code> otherwise.
	 */
	protected static boolean hasConfiguredNetworkInterface() {
		try {
			Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
			while (e.hasMoreElements()) {
				NetworkInterface i = e.nextElement();
				Enumeration<InetAddress> as = i.getInetAddresses();
				while (as.hasMoreElements()) {
					InetAddress a = as.nextElement();
					if (a instanceof Inet4Address && !a.isLoopbackAddress()
							&& !a.isMulticastAddress()) {
						return true;
					}
				}
			}
		}
		catch (Exception _) {
			// Silently cast the exceptions
		}
		return false;
	}
	
	/** Wait for the real termination of the Hazelcast instance.
	 * 
	 * @param hz
	 */
	protected static void waitForTermination(HazelcastInstance hz) {
		TestUtil.terminateInstance(hz);
		hz.shutdown(); // Only to be sure
		boolean hasMember = true;
		do {
			try {
				Member m = hz.getCluster().getLocalMember();
				hasMember = m!=null;
			}
			catch(Throwable _) {
				hasMember = false;
			}
		}
		while (hasMember);
	}

	private String useNetwork;
	private String multicastGroup;
	private HazelcastInstance hz1;
	private HazelcastInstance hz2;

	@Before
	public void setUpTests() {
		assumeFalse(
				"This test can be processed only if your host has no configured network interface.",
				hasConfiguredNetworkInterface());
		this.hz1 = this.hz2 = null;
		this.useNetwork = System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, Boolean.TRUE.toString());
		this.multicastGroup = System.clearProperty("hazelcast.multicast.group");
	}

	@After
	public void tearDownTests() {
		if (this.hz2!=null) waitForTermination(this.hz2);
		if (this.hz1!=null) waitForTermination(this.hz1);
		this.hz1 = this.hz2 = null;
		if (this.useNetwork==null)
			System.clearProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK);
		else
			System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, this.useNetwork);
		if (this.multicastGroup==null)
			System.clearProperty("hazelcast.multicast.group");
		else
			System.setProperty("hazelcast.multicast.group", this.multicastGroup);
	}	

	private void createTestEnvironment(boolean loopbackMode) throws Exception {
		Config config1 = new Config();
		{
			config1.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config1.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(loopbackMode);
		}

		Config config2 = new Config();
		{
			config2.setProperty("hazelcast.local.localAddress", "127.0.0.1"); //$NON-NLS-1$
			MulticastConfig multicastConfig = config2.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setLoopbackModeEnabled(loopbackMode);
		}

		this.hz1 = HazelcastInstanceFactory.newHazelcastInstance(config1);
		assertNotNull("cannot create the first hazelcastInstance",hz1);

		this.hz2 = HazelcastInstanceFactory.newHazelcastInstance(config2);
		assertNotNull("cannot create the first hazelcastInstance",hz2);
	}


	@Test
	public void testEnabledMode() throws Exception {
		String dataName = randomMapName(MulticastLoopbackModeTest.class.getName());
		String myID = randomString();

		createTestEnvironment(true);

		ISet<String> theMap = this.hz1.getSet(dataName);
		theMap.add(myID);


		ISet<String> remoteMap = this.hz2.getSet(dataName);

		sleepSeconds(1);

		assertTrue(remoteMap.contains(myID));
	}

	@Test
	public void testDisabledMode() throws Exception {
		String dataName = randomMapName(MulticastLoopbackModeTest.class.getName());
		String myID = randomString();

		createTestEnvironment(false);

		ISet<String> theMap = this.hz1.getSet(dataName);
		theMap.add(myID);


		ISet<String> remoteMap = this.hz2.getSet(dataName);

		sleepSeconds(1);

		assertFalse(remoteMap.contains(myID));
	}

}
