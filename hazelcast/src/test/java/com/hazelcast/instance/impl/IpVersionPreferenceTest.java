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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV4_STACK;
import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV6_ADDRESSES;
import static com.hazelcast.instance.impl.DefaultAddressPickerTest.findIPv6NonLoopbackInterface;
import static com.hazelcast.spi.properties.ClusterProperty.PREFER_IPv4_STACK;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IpVersionPreferenceTest {

    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");
    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv4Stack = clear(PREFER_IPV4_STACK);
    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv6Addresses = clear(PREFER_IPV6_ADDRESSES);
    @Rule
    public final OverridePropertyRule ruleSysPropPreferHzIpv4 = clear(PREFER_IPv4_STACK.getName());

    @Parameter
    public Boolean hazelcastIpv4;

    @Parameter(value = 1)
    public Boolean javaIpv4;

    @Parameter(value = 2)
    public Boolean javaIpv6;

    private static final Boolean[] BOOL_VALUES = new Boolean[] {Boolean.TRUE, Boolean.FALSE, null};

    @Parameters(name = "hazelcastIpv4:{0} javaIpv4:{1} javaIpv6:{2}")
    public static Collection<Object[]> parameters() {
        List<Object[]> params = new ArrayList<Object[]>();
        for (Boolean i : BOOL_VALUES) {
            for (Boolean j : BOOL_VALUES) {
                for (Boolean k : BOOL_VALUES) {
                    params.add(new Object[] {i, j, k});
                }
            }
        }
        return params;
    }

    @Test
    public void testBindAddress() throws Exception {
        ruleSysPropPreferHzIpv4.setOrClearProperty(hazelcastIpv4 == null ? null : String.valueOf(hazelcastIpv4));
        ruleSysPropPreferIpv4Stack.setOrClearProperty(javaIpv4 == null ? null : String.valueOf(javaIpv4));
        ruleSysPropPreferIpv6Addresses.setOrClearProperty(javaIpv6 == null ? null : String.valueOf(javaIpv6));

        boolean expectedIPv6 = !getOrDefault(hazelcastIpv4, true)
                        && !getOrDefault(javaIpv4, false)
                        && getOrDefault(javaIpv6, false);

        if (expectedIPv6) {
            assumeNotNull(findIPv6NonLoopbackInterface());
        }

        DefaultAddressPicker addressPicker = new DefaultAddressPicker(new Config(), Logger.getLogger(AddressPicker.class));
        try {
            addressPicker.pickAddress();
            Address bindAddress = addressPicker.getBindAddress(MEMBER);
            assertEquals("Bind address: " + bindAddress, expectedIPv6, bindAddress.isIPv6());
        } finally {
            IOUtil.closeResource(addressPicker.getServerSocketChannel(MEMBER));
        }
    }

    private boolean getOrDefault(Boolean value, boolean defaultValue) {
        return value != null ? value : defaultValue;
    }
}
