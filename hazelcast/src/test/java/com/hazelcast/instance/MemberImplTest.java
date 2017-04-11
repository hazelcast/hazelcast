/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberImplTest extends HazelcastTestSupport {

    private static HazelcastInstanceImpl hazelcastInstance;
    private static Address address;

    @BeforeClass
    public static void setUp() throws Exception {
        hazelcastInstance = new HazelcastInstanceImpl("test", new Config(), new DefaultNodeContext());
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
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, newUnsecureUuidString(), null, true);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertTrue(member.isLiteMember());
    }

    @Test
    public void testConstructor_withLiteMember_isFalse() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, newUnsecureUuidString(), null, false);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withHazelcastInstance() throws Exception {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, "uuid2342", null, false, hazelcastInstance);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals("uuid2342", member.getUuid());
    }

    @Test
    public void testConstructor_withAttributes() throws Exception {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("key1", "value");
        attributes.put("key2", 12345);

        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, "uuid2342", attributes, false, hazelcastInstance);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals("uuid2342", member.getUuid());
        assertEquals("value", member.getAttribute("key1"));
        assertEquals(12345, member.getAttribute("key2"));
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
        assertNull(member.getStringAttribute("stringKey"));

        member.setStringAttribute("stringKey", "stringValue");
        assertEquals("stringValue", member.getStringAttribute("stringKey"));
    }

    @Test
    public void testBooleanAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getBooleanAttribute("booleanKeyTrue"));
        assertNull(member.getBooleanAttribute("booleanKeyFalse"));

        member.setBooleanAttribute("booleanKeyTrue", true);
        assertTrue(member.getBooleanAttribute("booleanKeyTrue"));

        member.setBooleanAttribute("booleanKeyFalse", false);
        assertFalse(member.getBooleanAttribute("booleanKeyFalse"));
    }

    @Test
    public void testByteAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getByteAttribute("byteKey"));

        Byte value = Byte.MAX_VALUE;
        member.setByteAttribute("byteKey", value);
        assertEquals(value, member.getByteAttribute("byteKey"));
    }

    @Test
    public void testShortAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getShortAttribute("shortKey"));

        Short value = Short.MAX_VALUE;
        member.setShortAttribute("shortKey", value);
        assertEquals(value, member.getShortAttribute("shortKey"));
    }

    @Test
    public void testIntAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getIntAttribute("intKey"));

        Integer value = Integer.MAX_VALUE;
        member.setIntAttribute("intKey", value);
        assertEquals(value, member.getIntAttribute("intKey"));
    }

    @Test
    public void testLongAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getLongAttribute("longKey"));

        Long value = Long.MAX_VALUE;
        member.setLongAttribute("longKey", value);
        assertEquals(value, member.getLongAttribute("longKey"));
    }

    @Test
    public void testFloatAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getFloatAttribute("floatKey"));

        Float value = Float.MAX_VALUE;
        member.setFloatAttribute("floatKey", value);
        assertEquals(value, member.getFloatAttribute("floatKey"), 0.001);
    }

    @Test
    public void testDoubleAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getDoubleAttribute("doubleKey"));

        Double value = Double.MAX_VALUE;
        member.setDoubleAttribute("doubleKey", value);
        assertEquals(value, member.getDoubleAttribute("doubleKey"), 0.001);
    }

    @Test
    public void testRemoveAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getStringAttribute("removeKey"));

        member.setStringAttribute("removeKey", "removeValue");
        assertEquals("removeValue", member.getStringAttribute("removeKey"));

        member.removeAttribute("removeKey");
        assertNull(member.getStringAttribute("removeKey"));
    }

    @Test
    public void testRemoveAttribute_withHazelcastInstance() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, "uuid", null, false, hazelcastInstance);

        member.removeAttribute("removeKeyWithInstance");
        assertNull(member.getStringAttribute("removeKeyWithInstance"));
    }

    @Test
    public void testSetAttribute_withHazelcastInstance() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true, "uuid", null, false, hazelcastInstance);

        member.setStringAttribute("setKeyWithInstance", "setValueWithInstance");
        assertEquals("setValueWithInstance", member.getStringAttribute("setKeyWithInstance"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAttribute_onRemoteMember() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);
        member.removeAttribute("remoteMemberRemove");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute_onRemoteMember() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);
        member.setStringAttribute("remoteMemberSet", "wontWork");
    }

    private static void assertBasicMemberImplFields(MemberImpl member) {
        assertEquals(address, member.getAddress());
        assertEquals(5701, member.getPort());
        assertEquals("127.0.0.1", member.getInetAddress().getHostAddress());
        assertTrue(member.getFactoryId() > -1);
        assertTrue(member.getId() > -1);
        assertNotNull(member.getVersion());
        assertEquals(3, member.getVersion().getMajor());
        assertEquals(8, member.getVersion().getMinor());
        assertEquals(0, member.getVersion().getPatch());
    }
}
