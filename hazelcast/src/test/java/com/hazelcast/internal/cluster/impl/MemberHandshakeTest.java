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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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

import static com.hazelcast.internal.cluster.impl.MemberHandshake.SCHEMA_VERSION_2;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberHandshakeTest {

    private MemberHandshake originalHandshake;
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
    public void testOptions() throws Exception {
        originalHandshake = new MemberHandshake(SCHEMA_VERSION_2, localAddresses(), targetAddress, true, uuid)
                .addOption("foo", 1)
                .addOption("bar", 2);

        MemberHandshake clonedHandshake = cloneHandshake(originalHandshake);
        assertEquals(SCHEMA_VERSION_2, clonedHandshake.getSchemaVersion());
        assertEquals(localAddresses(), clonedHandshake.getLocalAddresses());
        assertEquals(targetAddress, clonedHandshake.getTargetAddress());
        assertTrue(clonedHandshake.isReply());
        assertEquals(uuid, clonedHandshake.getUuid());
        assertEquals(1, clonedHandshake.getIntOption("foo", -1));
        assertEquals(2, clonedHandshake.getIntOption("bar", -1));
    }

    public MemberHandshake cloneHandshake(MemberHandshake handshake) {
        Data serialized = serializationService.toData(handshake);
        return serializationService.toObject(serialized);
    }

    @Test
    public void testSerialization_withMultipleLocalAddresses() throws Exception {
        originalHandshake = new MemberHandshake(SCHEMA_VERSION_2, localAddresses(), targetAddress, true, uuid);
        MemberHandshake clonedHandshake = cloneHandshake(originalHandshake);

        assertEquals(SCHEMA_VERSION_2, clonedHandshake.getSchemaVersion());
        assertEquals(localAddresses(), clonedHandshake.getLocalAddresses());
        assertEquals(targetAddress, clonedHandshake.getTargetAddress());
        assertTrue(clonedHandshake.isReply());
        assertEquals(uuid, clonedHandshake.getUuid());
    }

    @Test
    public void testSerialization_whenMemberHandshakeEmpty() {
        originalHandshake = new MemberHandshake();
        MemberHandshake clonedHandshake = cloneHandshake(originalHandshake);

        assertEquals(0, clonedHandshake.getSchemaVersion());
        assertTrue(clonedHandshake.getLocalAddresses().isEmpty());
        assertNull(null, clonedHandshake.getTargetAddress());
        assertFalse(clonedHandshake.isReply());
        assertNull(clonedHandshake.getUuid());
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
