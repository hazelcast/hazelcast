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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientFailoverConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddClientConfig_WithOffReconnectMode_ShouldThrowInvalidConfigException() {
        ClientFailoverConfig failoverConfig = new ClientFailoverConfig();
        assertThat(failoverConfig.getClientConfigs(), is(empty()));

        ClientConfig clientConfig1 = new ClientConfig();
        failoverConfig.addClientConfig(clientConfig1);
        assertThat(failoverConfig.getClientConfigs(), hasSize(1));

        ClientConfig clientConfig2 = new ClientConfig()
                .setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setReconnectMode(OFF));

        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Reconnect mode for ClientFailoverConfig must not be OFF");
        failoverConfig.addClientConfig(clientConfig2);
    }

    @Test
    public void testSetClientConfigs_WithOffReconnectMode_ShouldThrowInvalidConfigException() {
        ClientConfig clientConfig1 = new ClientConfig();
        ClientConfig clientConfig2 = new ClientConfig()
                .setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setReconnectMode(OFF));

        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Reconnect mode for ClientFailoverConfig must not be OFF");
        new ClientFailoverConfig().setClientConfigs(Arrays.asList(clientConfig1, clientConfig2));
    }

}
