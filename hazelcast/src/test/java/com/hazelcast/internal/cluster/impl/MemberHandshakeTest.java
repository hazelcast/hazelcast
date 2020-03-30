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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberHandshakeTest {

    private MemberHandshake bindMessage;
    private SerializationService serializationService;
    private Address targetAddress;
    private UUID uuid;

    @Before
    public void setup() throws UnknownHostException {
        targetAddress = new Address("127.0.0.1", 9999);
        serializationService = new DefaultSerializationServiceBuilder().build();
        uuid = UUID.randomUUID();
    }

    @Test
    public void testSerialization_withMultipleLocalAddresses() throws Exception {
        bindMessage = new MemberHandshake((byte) 1, localAddresses(), targetAddress, true, uuid);
        Data serialized = serializationService.toData(bindMessage);
        MemberHandshake deserialized = serializationService.toObject(serialized);
        assertEquals(1, deserialized.getSchemaVersion());
        assertEquals(localAddresses(), deserialized.getLocalAddresses());
        assertEquals(targetAddress, deserialized.getTargetAddress());
        assertTrue(deserialized.isReply());
        assertEquals(uuid, deserialized.getUuid());
    }

    @Test
    public void testSerialization_whenBindMessageEmpty() {
        bindMessage = new MemberHandshake();
        Data serialized = serializationService.toData(bindMessage);
        MemberHandshake deserialized = serializationService.toObject(serialized);
        assertEquals(0, deserialized.getSchemaVersion());
        assertTrue(deserialized.getLocalAddresses().isEmpty());
        assertNull(null, deserialized.getTargetAddress());
        assertFalse(deserialized.isReply());
        assertNull(deserialized.getUuid());
    }

    Map<ProtocolType, Collection<Address>> localAddresses() throws Exception {
        Map<ProtocolType, Collection<Address>> map = new EnumMap<>(ProtocolType.class);
        Collection<Address> addresses = new ArrayList<>();
        addresses.add(new Address("127.0.0.1", 5701));
        addresses.add(new Address("127.0.0.1", 5702));
        addresses.add(new Address("127.0.0.1", 5703));
        map.put(ProtocolType.WAN, addresses);
        map.put(ProtocolType.MEMBER, singletonList(new Address("127.0.0.1", 5801)));
        map.put(ProtocolType.CLIENT, singletonList(new Address("127.0.0.1", 5802)));
        map.put(ProtocolType.REST, singletonList(new Address("127.0.0.1", 5803)));
        return map;
    }
}
