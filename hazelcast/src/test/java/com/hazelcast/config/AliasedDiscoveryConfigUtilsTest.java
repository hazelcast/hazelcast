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

import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AliasedDiscoveryConfigUtilsTest {

    @Test
    public void supports() {
        assertTrue(AliasedDiscoveryConfigUtils.supports("gcp"));
        assertFalse(AliasedDiscoveryConfigUtils.supports("unknown"));
    }

    @Test
    public void tagFor() {
        assertEquals("gcp", AliasedDiscoveryConfigUtils.tagFor(new GcpConfig()));
        assertEquals("aws", AliasedDiscoveryConfigUtils.tagFor(new AwsConfig()));
        assertEquals("aws", AliasedDiscoveryConfigUtils.tagFor(new AwsConfig() {
        }));
        assertNull(AliasedDiscoveryConfigUtils.tagFor(new DummyAliasedDiscoveryConfig(null)));
    }

    @Test
    public void createDiscoveryStrategyConfigsFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();
        config.getGcpConfig().setEnabled(true);

        // when
        List<DiscoveryStrategyConfig> result = AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        // then
        assertEquals(1, result.size());
        assertEquals("com.hazelcast.gcp.GcpDiscoveryStrategy", result.get(0).getClassName());
    }

    @Test
    public void createDiscoveryStrategyConfigsFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();
        config.getGcpConfig().setEnabled(true);

        // when
        List<DiscoveryStrategyConfig> result = AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        // then
        assertEquals(1, result.size());
        assertEquals("com.hazelcast.gcp.GcpDiscoveryStrategy", result.get(0).getClassName());
    }

    @Test
    public void map() {
        // given
        List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigs = new ArrayList<>();
        aliasedDiscoveryConfigs
                .add(new GcpConfig().setEnabled(true).setProperty("projects", "hazelcast-33").setProperty("zones", "us-east1-b"));
        aliasedDiscoveryConfigs.add(new AwsConfig() { }.setEnabled(true).setProperty("access-key", "someAccessKey")
                .setProperty("secret-key", "someSecretKey").setProperty("region", "eu-central-1"));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(aliasedDiscoveryConfigs);

        // then
        DiscoveryStrategyConfig gcpConfig = discoveryConfigs.get(0);
        assertEquals("com.hazelcast.gcp.GcpDiscoveryStrategy", gcpConfig.getClassName());
        assertEquals("hazelcast-33", gcpConfig.getProperties().get("projects"));
        assertEquals("us-east1-b", gcpConfig.getProperties().get("zones"));

        DiscoveryStrategyConfig awsConfig = discoveryConfigs.get(1);
        assertEquals("com.hazelcast.aws.AwsDiscoveryStrategy", awsConfig.getClassName());
        assertEquals("someAccessKey", awsConfig.getProperties().get("access-key"));
        assertEquals("someSecretKey", awsConfig.getProperties().get("secret-key"));
        assertEquals("eu-central-1", awsConfig.getProperties().get("region"));
    }

    @Test
    public void skipNotEnabledConfigs() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(new GcpConfig().setEnabled(false));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(configs);

        // then
        assertTrue(discoveryConfigs.isEmpty());
    }

    @Test
    public void skipPropertyWithNullKey() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(new GcpConfig().setEnabled(true).setProperty(null, "value"));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(configs);

        // then
        assertTrue(discoveryConfigs.get(0).getProperties().isEmpty());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void validateUnknownEnvironments() {
        // given
        AliasedDiscoveryConfig aliasedDiscoveryConfig = new DummyAliasedDiscoveryConfig("invalid-tag") {
        }.setEnabled(true);
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(aliasedDiscoveryConfig);

        // when
        AliasedDiscoveryConfigUtils.map(configs);

        // then
        // throws exception
    }

    @Test
    public void getConfigByTagFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();

        // when
        AliasedDiscoveryConfig result = AliasedDiscoveryConfigUtils.getConfigByTag(config, "gcp");

        // then
        assertEquals(GcpConfig.class, result.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getConfigByInvalidTagFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();

        // when
        AliasedDiscoveryConfigUtils.getConfigByTag(config, "unknown");

        // then
        // throw exception
    }

    @Test
    public void getConfigByTagFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();

        // when
        AliasedDiscoveryConfig result = AliasedDiscoveryConfigUtils.getConfigByTag(config, "gcp");

        // then
        assertEquals(GcpConfig.class, result.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getConfigByInvalidTagFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();

        // when
        AliasedDiscoveryConfigUtils.getConfigByTag(config, "unknown");

        // then
        // throw exception
    }

    @Test
    public void allUsePublicAddressTrue() {
        // given
        AwsConfig awsConfig = new AwsConfig().setEnabled(true).setUsePublicIp(true);
        GcpConfig gcpConfig = new GcpConfig().setEnabled(true).setUsePublicIp(true);
        List<AliasedDiscoveryConfig<?>> configs = asList(awsConfig, gcpConfig);

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertTrue(result);
    }

    @Test
    public void allUsePublicAddressFalse() {
        // given
        AwsConfig awsConfig = new AwsConfig().setEnabled(true).setUsePublicIp(true);
        GcpConfig gcpConfig = new GcpConfig().setEnabled(true).setUsePublicIp(false);
        List<AliasedDiscoveryConfig<?>> configs = asList(awsConfig, gcpConfig);

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertFalse(result);
    }

    @Test
    public void allUsePublicAddressEmpty() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertFalse(result);
    }

    private static class DummyAliasedDiscoveryConfig extends AliasedDiscoveryConfig {
        protected DummyAliasedDiscoveryConfig(String tag) {
            super(tag);
        }

        @Override
        public int getClassId() {
            throw new UnsupportedOperationException("Deserialization not supported!");
        }
    }
}
