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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestLogLevelTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return HazelcastTestSupport.smallInstanceConfig();
    }

    protected Config createReadWriteConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ).enableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createReadOnlyConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ).disableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createWriteOnlyConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).disableGroups(RestEndpointGroup.CLUSTER_READ).enableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected String getPassword() {
        return "";
    }

    @Test
    public void testDisabledRest() {
        // REST should be disabled by default
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, communicator::getLogLevel);
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterRead() {
        HazelcastInstance instance = factory.newHazelcastInstance(createWriteOnlyConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, communicator::getLogLevel);
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterWrite() {
        Config config = createReadOnlyConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, () -> communicator.setLogLevel(config.getClusterName(), getPassword(), Level.FINE));
    }

    @Test
    public void testGetSetResetLogLevel() throws IOException {
        Config config = createReadWriteConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        HTTPCommunicator.ConnectionResponse response = communicator.getLogLevel();
        JsonValue jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertTrue(jsonValue.isNull());

        response = communicator.setLogLevel(config.getClusterName(), getPassword(), Level.FINE);
        jsonValue = Json.parse(response.response).asObject().get("message");
        assertEquals("log level is changed", jsonValue.asString());

        response = communicator.getLogLevel();
        jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertEquals(Level.FINE.getName(), jsonValue.asString());

        response = communicator.resetLogLevel(config.getClusterName(), getPassword());
        jsonValue = Json.parse(response.response).asObject().get("message");
        assertEquals("log level is reset", jsonValue.asString());

        response = communicator.getLogLevel();
        jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertTrue(jsonValue.isNull());
    }

}
