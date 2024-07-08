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

package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMemberClientInvalidConfigurationExceptionTest extends ClientTestSupport {

    private TestHazelcastFactory factory;
    private static final String EXPECTED_MSG = "MULTI_MEMBER routing is an enterprise feature since 5.5. "
            +  "You must use Hazelcast enterprise to enable this feature.";

    @Before
    public void before() throws IOException {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testMultiMemberClientThrowsInvalidConfigurationException() {
        // create a multi member client config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.MULTI_MEMBER);

        // Check that we get an UnsupportedOperationException when trying to create client
        // with MULTI_MEMBER routing enabled and no member group defined
        assertThatThrownBy(() -> factory.newHazelcastClient(clientConfig))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessage(EXPECTED_MSG);
    }

}
