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

package com.hazelcast.console;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

/**
 * End-to-end test(s) for {@link ConsoleApp}. The tests use real network.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class })
public class ConsoleTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public OverridePropertyRule propertyClientConfig = OverridePropertyRule.clear("hazelcast.config");

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void connectsToHazelcastCluster() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance(null);
        Address address = getAddress(hz);
        File cfgFile = tempFolder.newFile("hazelcast-config.yml");
        try (BufferedWriter writer = Files.newBufferedWriter(cfgFile.toPath())) {
            writer.write("hazelcast:\n"
                    + "  cluster-name: " + getTestMethodName() + "\n"
                    + "  instance-name: consoleApp\n"
                    + "  network:\n"
                    + "    join:\n"
                    + "      multicast:\n"
                    + "        enabled: false\n"
                    + "      tcp-ip:\n"
                    + "        enabled: true\n"
                    + "        member-list:\n"
                    + "          - " + address.getHost() + ":" + address.getPort() + "\n");
        }
        propertyClientConfig.setOrClearProperty(cfgFile.getAbsolutePath());
        assertTrue(hz.getClientService().getConnectedClients().isEmpty());
        InputStream oldSysIn = null;
        PipedInputStream pipeIn = null;
        PipedOutputStream pipeOut = null;
        try {
            oldSysIn = System.in;
            pipeIn = new PipedInputStream(1024);
            pipeOut = new PipedOutputStream(pipeIn);
            System.setIn(pipeIn);
            ConsoleApp consoleApp = ConsoleApp.create();
            ExecutorService tp = Executors.newFixedThreadPool(1);
            try {
                tp.execute(() -> {
                    try {
                        consoleApp.start();
                    } catch (Exception e) {
                        sneakyThrow(e);
                    }
                });
                assertTrueEventually(() -> assertTrue(consoleApp.isRunning()));
                assertClusterSizeEventually(2, hz);
                HazelcastInstance hzConsole = Hazelcast.getHazelcastInstanceByName("consoleApp");
                assertNotNull(hzConsole);
                hzConsole.shutdown();
            } catch (Exception e) {
                sneakyThrow(e);
            } finally {
                int terminationTimeoutSec = 10;
                consoleApp.stop();
                // write new line char to sys.in so that BufferedReader#readLine completes
                String lineSep = System.getProperty("line.separator");
                pipeOut.write(lineSep.getBytes(StandardCharsets.UTF_8));
                tp.shutdownNow();
                boolean terminated = tp.awaitTermination(terminationTimeoutSec, TimeUnit.SECONDS);
                assertTrue("Executor service: " + tp + " is not terminated in " + terminationTimeoutSec + " secs",
                        terminated);
            }
        } finally {
            if (pipeIn != null) {
                pipeIn.close();
            }
            if (pipeOut != null) {
                pipeOut.close();
            }
            System.setIn(oldSysIn);
        }
    }
}
