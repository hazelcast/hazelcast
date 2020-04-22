/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.console;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

/**
 * End-to-end test(s) for {@link ClientConsoleApp}. The tests use real network.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class })
public class ClientConsoleTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public OverridePropertyRule propertyClientConfig = OverridePropertyRule.clear("hazelcast.client.config");

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    /**
     * Tests if the application correctly connects to a Hazelcast cluster. The TCP discovery is configured by using a custom
     * YAML client configuration file.
     */
    @Test
    public void connectsToHazelcastCluster() throws IOException {
        HazelcastInstance hz = factory.newHazelcastInstance(smallInstanceConfig());
        Address address = getAddress(hz);
        File cfgFile = tempFolder.newFile("hazelcast-config.yml");
        try (BufferedWriter writer = Files.newBufferedWriter(cfgFile.toPath())) {
            writer.write("hazelcast-client:\n"
                    + "  cluster-name: " + getTestMethodName() + "\n"
                    + "  instance-name: clientConsoleApp\n"
                    + "  network:\n"
                    + "    cluster-members:\n"
                    + "      - " + address.getHost() + ":" + address.getPort() + "\n");
        }
        propertyClientConfig.setOrClearProperty(cfgFile.getAbsolutePath());
        assertTrue(hz.getClientService().getConnectedClients().isEmpty());

        ExecutorService tp = Executors.newFixedThreadPool(1);
        try {
            tp.execute(() -> ClientConsoleApp.main(null));
            assertTrueEventually(() -> assertFalse(hz.getClientService().getConnectedClients().isEmpty()));
            HazelcastInstance client = HazelcastClient.getHazelcastClientByName("clientConsoleApp");
            assertNotNull(client);
            client.shutdown();
        } finally {
            HazelcastClient.shutdownAll();
            tp.shutdown();
        }
    }
}
