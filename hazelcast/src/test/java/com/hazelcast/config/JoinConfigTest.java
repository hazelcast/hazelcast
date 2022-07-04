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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JoinConfigTest extends HazelcastTestSupport {

    @Test
    public void joinConfigTest() {
        assertOk(true, false, false, false, false, false, false, false, false);
        assertOk(true, true, false, false, false, false, false, false, false);
        assertOk(true, false, true, false, false, false, false, false, false);
        assertOk(true, false, false, true, false, false, false, false, false);
        assertOk(true, false, false, false, true, false, false, false, false);
        assertOk(true, false, false, false, false, true, false, false, false);
        assertOk(true, false, false, false, false, false, true, false, false);
        assertOk(true, false, false, false, false, false, false, true, false);
        assertOk(true, false, false, false, false, false, false, false, true);
        assertOk(false, false, false, false, false, false, false, false, true);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private static void assertOk(boolean autoDetection, boolean tcp, boolean multicast, boolean aws, boolean gcp, boolean azure,
                                 boolean kubernetes, boolean eureka, boolean discoveryConfig) {
        JoinConfig config = new JoinConfig();
        config.getAutoDetectionConfig().setEnabled(autoDetection);
        config.getMulticastConfig().setEnabled(multicast);
        config.getTcpIpConfig().setEnabled(tcp);
        config.getAwsConfig().setEnabled(aws);
        config.getGcpConfig().setEnabled(gcp);
        config.getAzureConfig().setEnabled(azure);
        config.getKubernetesConfig().setEnabled(kubernetes);
        config.getEurekaConfig().setEnabled(eureka);
        if (discoveryConfig) {
            config.getDiscoveryConfig().getDiscoveryStrategyConfigs().add(new DiscoveryStrategyConfig());
        }

        config.verify();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenTwoJoinMethodEnabled() {
        JoinConfig config = new JoinConfig();
        config.getMulticastConfig().setEnabled(true);
        config.getTcpIpConfig().setEnabled(true);

        config.verify();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenGcpAndAwsEnabled() {
        JoinConfig config = new JoinConfig();
        // Multicast enabled by default
        config.getMulticastConfig().setEnabled(false);
        config.getAwsConfig().setEnabled(true);
        config.getGcpConfig().setEnabled(true);

        config.verify();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenMulticastAndDiscoveryStrategyEnabled() {
        JoinConfig config = new JoinConfig();
        config.getMulticastConfig().setEnabled(true);
        config.getDiscoveryConfig().getDiscoveryStrategyConfigs().add(new DiscoveryStrategyConfig());

        config.verify();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenTcpIpAndDiscoveryStrategyEnabled() {
        JoinConfig config = new JoinConfig();
        // Multicast enabled by default
        config.getMulticastConfig().setEnabled(false);
        config.getTcpIpConfig().setEnabled(true);
        config.getDiscoveryConfig().getDiscoveryStrategyConfigs().add(new DiscoveryStrategyConfig());

        config.verify();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenEurekaAndDiscoveryStrategyEnabled() {
        JoinConfig config = new JoinConfig();
        // Multicast enabled by default
        config.getMulticastConfig().setEnabled(false);
        config.getEurekaConfig().setEnabled(true);
        config.getDiscoveryConfig().getDiscoveryStrategyConfigs().add(new DiscoveryStrategyConfig());

        config.verify();
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(JoinConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
