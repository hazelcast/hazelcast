/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberImplTest extends HazelcastTestSupport {

    private static final MemberVersion VERSION = MemberVersion.of("1.3.2");
    private static Address address;

    @BeforeClass
    public static void setUp() throws Exception {
        address = new Address("127.0.0.1", 5701);
    }

    @Test
    public void testConstructor() {
        MemberImpl member = new MemberImpl();

        assertNull(member.getAddress());
        assertFalse(member.localMember());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withAddress() {
        MemberImpl member = new MemberImpl(address, VERSION);

        assertBasicMemberImplFields(member);
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withAddressAndUUid() {
        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl(address, VERSION, uuid);

        assertBasicMemberImplFields(member);
        assertEquals(uuid, member.getUuid());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withAttributes() throws Exception {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("stringKey", "value");

        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl(address, VERSION, uuid, attributes, true);

        assertBasicMemberImplFields(member);
        assertEquals(uuid, member.getUuid());
        assertTrue(member.isLiteMember());

        assertEquals("value", member.getAttribute("stringKey"));
        assertNull(member.getAttribute("keydoesnotexist"));
    }

    @Test
    public void testConstructor_withMemberImpl() {
        MemberImpl member = new MemberImpl(new MemberImpl(address, VERSION));

        assertBasicMemberImplFields(member);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setAttribute("stringKey", "stringValue");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.removeAttribute("removeKey");
    }

    private static void assertBasicMemberImplFields(MemberImpl member) {
        assertEquals(address, member.getAddress());
        assertEquals(5701, member.getPort());
        assertEquals("127.0.0.1", member.getInetAddress().getHostAddress());
        assertNull(member.getLogger());
        assertFalse(member.localMember());
        assertEquals(1, member.getVersion().getMajor());
        assertEquals(3, member.getVersion().getMinor());
        assertEquals(2, member.getVersion().getPatch());
    }
}
