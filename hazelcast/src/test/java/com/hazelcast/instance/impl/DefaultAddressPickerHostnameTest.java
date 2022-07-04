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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultAddressPicker.HostnameResolver;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.DummyNetworkInterfacesEnumerator;
import com.hazelcast.internal.util.NetworkInterfaceInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV4_STACK;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultAddressPickerHostnameTest {

    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv4 = set(PREFER_IPV4_STACK, "true");
    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");

    private final ILogger logger = Logger.getLogger(AddressPicker.class);
    private final Config config = new Config();
    private final String theHostname = "hazelcast.istanbul";
    private final String theAddress = "10.34.34.0";
    private final DefaultAddressPicker addressPicker = new DefaultAddressPicker(config, logger);
    private final HostnameResolver hostnameResolver = new MockHostnameResolver();

    @Before
    public void before() {

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember(theHostname);

        addressPicker.setHostnameResolver(hostnameResolver);
    }

    @After
    public void after() {
        addressPicker.getServerSocketChannels().values().forEach(IOUtil::closeResource);
    }

    @Test
    public void whenHostnameIsLocal_thenSelectHostname() throws Exception {
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", "192.168.1.100");
        NetworkInterfaceInfo en1 = createNetworkInterface("en1", theAddress);
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0, en1);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        addressPicker.pickAddress();
        assertEquals(theHostname, addressPicker.getBindAddress(MEMBER).getHost());
    }

    @Test
    public void whenHostnameIsNotLocal_thenSelectAnotherAddress() throws Exception {
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", "192.168.1.100");
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        addressPicker.pickAddress();
        assertNotEquals(theHostname, addressPicker.getBindAddress(MEMBER).getHost());
    }

    @Test
    public void whenHostnameIsLocal_andInterfacesMatchingHostname_thenSelectHostname() throws Exception {
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", "192.168.1.100");
        NetworkInterfaceInfo en1 = createNetworkInterface("en1", theAddress);
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0, en1);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        enableInterfacesConfig("10.34.34.*");

        addressPicker.pickAddress();
        assertEquals(theHostname, addressPicker.getBindAddress(MEMBER).getHost());
    }

    @Test
    public void whenHostnameIsLocal_andInterfacesNotMatchingAny_thenFail() throws Exception {
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", "192.168.1.100");
        NetworkInterfaceInfo en1 = createNetworkInterface("en1", theAddress);
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0, en1);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        enableInterfacesConfig("10.34.19.*");

        try {
            addressPicker.pickAddress();
            fail("Address selection should fail, since no matching network interface found.");
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void whenHostnameIsNotLocal_andInterfacesMatchingHostname_thenFail() throws Exception {
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", "192.168.1.100");
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        enableInterfacesConfig("10.34.*.*");

        try {
            addressPicker.pickAddress();
            fail("Address selection should fail, since no matching network interface found.");
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void whenHostnameIsNotLocal_andInterfacesMatchingAnother_thenSelectAnotherAddress() throws Exception {
        String address = "192.168.1.100";
        NetworkInterfaceInfo en0 = createNetworkInterface("en0", address);
        DummyNetworkInterfacesEnumerator interfacesInfoProvider = new DummyNetworkInterfacesEnumerator(en0);
        addressPicker.setNetworkInterfacesEnumerator(interfacesInfoProvider);

        enableInterfacesConfig("192.168.*.*");

        addressPicker.pickAddress();
        assertEquals(address, addressPicker.getBindAddress(MEMBER).getHost());
    }

    private void enableInterfacesConfig(String pattern) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        InterfacesConfig interfacesConfig = networkConfig.getInterfaces();
        interfacesConfig.setEnabled(true).addInterface(pattern);
    }

    private static NetworkInterfaceInfo createNetworkInterface(String name, String... addresses) {
        return NetworkInterfaceInfo.builder(name).withUp(true).withVirtual(false).withLoopback(false).withAddresses(addresses).build();
    }

    private class MockHostnameResolver implements HostnameResolver {
        @Override
        public Collection<String> resolve(String hostname) throws UnknownHostException {
            if (theHostname.equals(hostname)) {
                return singleton(theAddress);
            }
            throw new UnknownHostException(hostname);
        }
    }
}
