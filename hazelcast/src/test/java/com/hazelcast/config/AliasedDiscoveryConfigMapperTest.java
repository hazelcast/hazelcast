/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AliasedDiscoveryConfigMapperTest {
    @Test
    public void map() {
        // given
        List<AliasedDiscoveryConfig> aliasedDiscoveryConfigs = new ArrayList<AliasedDiscoveryConfig>();
        aliasedDiscoveryConfigs
                .add(new AliasedDiscoveryConfig()
                        .setEnabled(true)
                        .setEnvironment("gcp")
                        .addProperty("projects", "hazelcast-33")
                        .addProperty("zones", "us-east1-b")
                );
        aliasedDiscoveryConfigs.add(new AliasedDiscoveryConfig()
                .setEnabled(true)
                .setEnvironment("aws")
                .addProperty("access-key", "someAccessKey")
                .addProperty("secret-key", "someSecretKey")
                .addProperty("region", "eu-central-1")
        );

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigMapper.map(aliasedDiscoveryConfigs);

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
        List<AliasedDiscoveryConfig> aliasedDiscoveryConfigs = Collections.singletonList(
                new AliasedDiscoveryConfig().setEnvironment("gcp").setEnabled(false));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigMapper.map(aliasedDiscoveryConfigs);

        // then
        assertTrue(discoveryConfigs.isEmpty());
    }

    @Test
    public void skipPropertyWithNullKey() {
        // given
        List<AliasedDiscoveryConfig> aliasedDiscoveryConfigs = Collections.singletonList(
                new AliasedDiscoveryConfig().setEnvironment("gcp").setEnabled(true).addProperty(null, "value"));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigMapper.map(aliasedDiscoveryConfigs);

        // then
        assertTrue(discoveryConfigs.get(0).getProperties().isEmpty());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void validateUnknownEnvironments() {
        // given
        List<AliasedDiscoveryConfig> aliasedDiscoveryConfigs = Collections.singletonList(
                new AliasedDiscoveryConfig().setEnvironment("unknown").setEnabled(true));

        // when
        AliasedDiscoveryConfigMapper.map(aliasedDiscoveryConfigs);

        // then
        // throws exception
    }
}
