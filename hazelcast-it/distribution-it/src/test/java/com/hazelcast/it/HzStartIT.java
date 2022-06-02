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

package com.hazelcast.it;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class HzStartIT extends HazelcastTestSupport {

    private static final ILogger log = Logger.getLogger(HzStartIT.class);

    private Process process;
    private HazelcastInstance client;
    private String clusterName;

    @Before
    public void setUp() throws Exception {
        assumeThatNoWindowsOS();

        // Use custom cluster name to isolate concurrently running tests
        clusterName = this.getClass().getSimpleName() + "_" + UUID.randomUUID();

        ProcessBuilder builder = new ProcessBuilder("bin/hz", "start")
                .directory(new File("./target/hazelcast"))
                .inheritIO();
        builder.environment().put("HZ_CLUSTERNAME", clusterName);
        builder.environment().put("JAVA_OPTS", "-Dhazelcast.phone.home.enabled=false");
        // Remove classpath set by Maven
        builder.environment().remove("CLASSPATH");

        process = builder.start();
        client = HazelcastClient.newHazelcastClient(new ClientConfig().setClusterName(clusterName));
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
        log.info("Destroying Hazelcast process");
        process.destroy();
        boolean destroyed = process.waitFor(30, TimeUnit.SECONDS);
        if (!destroyed) {
            log.info("Hazelcast process not destroyed, trying Process#destroyForcibly()");
            process.destroyForcibly()
                   .waitFor();
        }
    }

    @Test
    public void shouldStartHazelcastWithHzStart() {
        IMap<String, Integer> map = client.getMap("map");
        map.put("key", 42);
        assertThat(map.get("key")).isEqualTo(42);
    }
}
