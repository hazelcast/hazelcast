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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.RestoreSystemProperties;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks whether the mock network is compatible with the real network behavior.
 * @implNote This test cannot be ParallelJVMTest because it uses the real network.
 */
class MockServerEquivalenceTest {

    private TestHazelcastInstanceFactory factory;

    @AfterEach
    void tearDown() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @RestoreSystemProperties
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transmitShouldRejectLocalPacket(boolean realNetwork) {
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, String.valueOf(realNetwork));

        // TestHazelcastInstanceFactory depends on HAZELCAST_TEST_USE_NETWORK setting
        factory = new TestHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance();

        var connection = Accessors.getNode(instance).getServer().getConnectionManager(MEMBER);
        Packet packet = new Packet(Accessors.getSerializationService(instance).toBytes("dummy"))
                .setPacketType(Packet.Type.NULL);
        assertThat(connection.transmit(packet, Accessors.getAddress(instance))).isFalse();
    }
}
