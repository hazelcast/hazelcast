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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collections;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SplitBrainJoinMessageSerializationTest {

    SplitBrainJoinMessage message;
    SerializationService serializationService;
    Address address;
    ConfigCheck configCheck;
    MemberVersion currentMemberVersion;

    @Before
    public void setup() throws UnknownHostException {
        serializationService = createSerializationService();
        address = new Address("127.0.0.1", 5071);
        configCheck = new ConfigCheck();
        currentMemberVersion = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    }

    @Test
    public void testSerialization() {
        message = new SplitBrainJoinMessage((byte) 1, 2, currentMemberVersion, address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_10, 17);
        assertSplitBrainJoinMessage(true);
    }

    SerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    void assertSplitBrainJoinMessage(boolean assertMemberListVersion) {
        Data serialized = serializationService.toData(message);
        SplitBrainJoinMessage actual = serializationService.toObject(serialized);
        assertEquals(message.getAddress(), actual.getAddress());
        assertEquals(message.getClusterVersion(), actual.getClusterVersion());
        assertEquals(message.getUuid(), actual.getUuid());
        assertEquals(message.getMemberCount(), actual.getMemberCount());
        assertEquals(message.getMemberVersion(), actual.getMemberVersion());
        assertEquals(message.getPacketVersion(), actual.getPacketVersion());
        assertEquals(message.getBuildNumber(), actual.getBuildNumber());
        assertEquals(message.getDataMemberCount(), actual.getDataMemberCount());
        if (assertMemberListVersion) {
            assertEquals(message.getMemberListVersion(), actual.getMemberListVersion());
        }
    }
}
