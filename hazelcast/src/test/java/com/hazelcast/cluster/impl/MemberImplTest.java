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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getHazelcastInstanceImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberImplTest extends HazelcastTestSupport {

    private static HazelcastInstanceImpl hazelcastInstance;
    private static Address address;

    @BeforeClass
    public static void setUp() throws Exception {
        HazelcastInstance instance = new TestHazelcastInstanceFactory().newHazelcastInstance();
        hazelcastInstance = getHazelcastInstanceImpl(instance);
        address = new Address("127.0.0.1", 5701);
    }

    @AfterClass
    public static void tearDown() {
        hazelcastInstance.shutdown();
    }

    @Test
    public void testConstructor_withLocalMember_isTrue() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
    }

    @Test
    public void testConstructor_withLocalMember_isFalse() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);

        assertBasicMemberImplFields(member);
        assertFalse(member.localMember());
    }

    @Test
    public void testConstructor_withLiteMember_isTrue() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .liteMember(true)
                .build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertTrue(member.isLiteMember());
    }

    @Test
    public void testConstructor_withLiteMember_isFalse() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withHazelcastInstance() throws Exception {
        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl.Builder(address).version(MemberVersion.of("3.8.0"))
                .localMember(true).uuid(uuid).instance(hazelcastInstance).build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals(uuid, member.getUuid());
    }

    @Test
    public void testConstructor_withAttributes() throws Exception {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value");
        attributes.put("key2", "12345");

        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl.Builder(address).version(MemberVersion.of("3.8.0")).localMember(true)
                .uuid(uuid).attributes(attributes).instance(hazelcastInstance).build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals(uuid, member.getUuid());
        assertEquals("value", member.getAttribute("key1"));
        assertEquals("12345", member.getAttribute("key2"));
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withMemberImpl() {
        MemberImpl member = new MemberImpl(new MemberImpl(address, MemberVersion.of("3.8.0"), true));

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
    }

    @Test
    public void testSetHazelcastInstance() throws Exception {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getLogger());

        member.setHazelcastInstance(hazelcastInstance);

        assertNotNull(member.getLogger());
    }

    @Test
    public void testStringAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getAttribute("stringKey"));

        member.setAttribute("stringKey", "stringValue");
        assertEquals("stringValue", member.getAttribute("stringKey"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute_withHazelcastInstance() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .instance(hazelcastInstance).build();

        member.setAttribute("setKeyWithInstance", "setValueWithInstance");
        assertEquals("setValueWithInstance", member.getAttribute("setKeyWithInstance"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute_onRemoteMember() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);
        member.setAttribute("remoteMemberSet", "wontWork");
    }

    @Test
    public void testSerialization_whenSingleAddress() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.12.0"), false);
        testSerialization(member);
    }

    @Test
    public void testSerialization_whenMultiAddress() throws Exception {
        Map<EndpointQualifier, Address> addressMap = new HashMap<EndpointQualifier, Address>();
        addressMap.put(EndpointQualifier.MEMBER, address);
        addressMap.put(EndpointQualifier.REST, new Address("127.0.0.1", 8080));
        MemberImpl member = new MemberImpl.Builder(addressMap).version(MemberVersion.of("3.12.0")).build();
        testSerialization(member);
    }

    private void testSerialization(Member member) {
        SerializationService serializationService = getSerializationService(hazelcastInstance);
        Data serialized = serializationService.toData(member);
        Member deserialized = serializationService.toObject(serialized);
        assertEquals(member, deserialized);
    }

    private static void assertBasicMemberImplFields(MemberImpl member) {
        assertEquals(address, member.getAddress());
        assertEquals(5701, member.getPort());
        assertEquals("127.0.0.1", member.getInetAddress().getHostAddress());
        assertTrue(member.getFactoryId() > -1);
        assertTrue(member.getClassId() > -1);
        assertNotNull(member.getVersion());
        assertEquals(3, member.getVersion().getMajor());
        assertEquals(8, member.getVersion().getMinor());
        assertEquals(0, member.getVersion().getPatch());
    }
}
