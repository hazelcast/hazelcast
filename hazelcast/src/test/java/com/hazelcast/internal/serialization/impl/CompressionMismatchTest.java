/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.assertj.core.api.Assertions.assertThatCode;

@SerialTest
@QuickTest
public class CompressionMismatchTest {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    public record SampleSerializable (int x) implements Serializable {
    }

    @AfterEach
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void handlesCompressionMisconfiguration() {
        Config withCompression = smallInstanceConfig();
        withCompression.getSerializationConfig().setEnableCompression(true);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        clientConfig.getNetworkConfig().setAddresses(List.of("127.0.0.1:5701"));
        clientConfig.getSerializationConfig().setEnableCompression(false);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(withCompression);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getMap("testMap").put(1, new SampleSerializable(10));
        assertThatCode(() -> server.getMap("testMap").get(1))
            .isInstanceOf(HazelcastSerializationException.class)
            .hasMessageContaining("Possible mismatch in serialization compression configuration");
    }

}
