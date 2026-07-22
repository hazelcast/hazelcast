/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.JavaIPvXProperties;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MembersJoinWithMulticastIT
        extends HazelcastTestSupport {
    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");

    @Parameter
    public String networkInterface;

    @Parameter(1)
    public boolean isIpv6;

    private final String clusterName = getClass().getSimpleName() + "_" + UuidUtil.newUnsecureUuidString();

    @Parameters(name = "{0}")
    public static List<Object[]> loadAllInterfaces()
            throws SocketException {
        List<Object[]> parameters = new ArrayList<>();
        parameters.add(new Object[]{null, false});
        List<NetworkInterface> interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        for (NetworkInterface ni : interfaces) {
            if (!ni.isUp() || !ni.supportsMulticast() || ni.isVirtual() || ni.isPointToPoint()) {
                continue;
            }
            for (InetAddress address : Collections.list(ni.getInetAddresses())) {
                boolean isIPv6 = address instanceof Inet6Address;
                if (isIPv6 && JavaIPvXProperties.INSTANCE.preferIPv4Stack()) {
                    continue;
                }
                parameters.add(new Object[]{address.getHostAddress(), isIPv6});
            }
        }
        return parameters;
    }

    @Test
    public void testCreatingMembers() {
        assertClusterSizeEquals(getBaseConfiguration(), 2);
    }

    @Test
    public void clusterFailsToFormWithLoopbackModeDisabled() {
        assumeTrue(OS.isMac());
        Config cfg = getBaseConfiguration();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setLoopbackModeEnabled(false);
        assertClusterSizeEquals(cfg, 1);
    }

    private Config getBaseConfiguration() {
        Config cfg = smallInstanceConfigWithoutJetAndMetrics().setClusterName(clusterName);
        if (networkInterface != null) {
            cfg.getNetworkConfig().getInterfaces().addInterface(networkInterface).setEnabled(true);
            if (isIpv6) {
                cfg.setProperty(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");
            }
        }
        return cfg;
    }

    private void assertClusterSizeEquals(Config cfg, int expected) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance master = factory.newHazelcastInstance(cfg);
        HazelcastInstance second = factory.newHazelcastInstance(cfg);
        waitAllForSafeState(master, second);
        assertClusterSize(expected, master, second);
    }
}
