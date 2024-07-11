/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientFailoverConfigTest {


    @Test
    public void testAddClientConfig_WithOffReconnectMode_ShouldThrowInvalidConfigException() {
        ClientFailoverConfig failoverConfig = new ClientFailoverConfig();
        assertThat(failoverConfig.getClientConfigs()).isEmpty();

        ClientConfig clientConfig1 = new ClientConfig();
        failoverConfig.addClientConfig(clientConfig1);
        assertThat(failoverConfig.getClientConfigs()).hasSize(1);

        ClientConfig clientConfig2 = new ClientConfig()
                .setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setReconnectMode(OFF));

        assertThatThrownBy(() -> failoverConfig.addClientConfig(clientConfig2))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Reconnect mode for ClientFailoverConfig must not be OFF");
    }

    @Test
    public void testSetClientConfigs_WithOffReconnectMode_ShouldThrowInvalidConfigException() {
        ClientConfig clientConfig1 = new ClientConfig();
        ClientConfig clientConfig2 = new ClientConfig()
                .setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setReconnectMode(OFF));

        List<ClientConfig> clientConfigList = Arrays.asList(clientConfig1, clientConfig2);
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        assertThatThrownBy(() -> clientFailoverConfig.setClientConfigs(clientConfigList))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Reconnect mode for ClientFailoverConfig must not be OFF");
    }
}
