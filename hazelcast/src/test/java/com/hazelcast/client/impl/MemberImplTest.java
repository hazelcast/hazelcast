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

package com.hazelcast.client.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        MemberImpl member = new MemberImpl(address, VERSION, "uuid2342");

        assertBasicMemberImplFields(member);
        assertEquals("uuid2342", member.getUuid());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withAttributes() throws Exception {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("stringKey", "value");
        attributes.put("booleanKeyTrue", true);
        attributes.put("booleanKeyFalse", false);
        attributes.put("byteKey", Byte.MAX_VALUE);
        attributes.put("shortKey", Short.MAX_VALUE);
        attributes.put("intKey", Integer.MAX_VALUE);
        attributes.put("longKey", Long.MAX_VALUE);
        attributes.put("floatKey", Float.MAX_VALUE);
        attributes.put("doubleKey", Double.MAX_VALUE);

        MemberImpl member = new MemberImpl(address, VERSION, "uuid2342", attributes, true);

        assertBasicMemberImplFields(member);
        assertEquals("uuid2342", member.getUuid());
        assertTrue(member.isLiteMember());

        assertEquals("value", member.getStringAttribute("stringKey"));
        assertNull(member.getBooleanAttribute("booleanKey"));

        Boolean booleanValueTrue = member.getBooleanAttribute("booleanKeyTrue");
        assertNotNull(booleanValueTrue);
        assertTrue(booleanValueTrue);

        Boolean booleanValueFalse = member.getBooleanAttribute("booleanKeyFalse");
        assertNotNull(booleanValueFalse);
        assertFalse(booleanValueFalse);

        Byte byteValue = member.getByteAttribute("byteKey");
        assertNotNull(byteValue);
        assertEquals(Byte.MAX_VALUE, byteValue.byteValue());

        Short shortValue = member.getShortAttribute("shortKey");
        assertNotNull(shortValue);
        assertEquals(Short.MAX_VALUE, shortValue.shortValue());

        Integer intValue = member.getIntAttribute("intKey");
        assertNotNull(intValue);
        assertEquals(Integer.MAX_VALUE, intValue.intValue());

        Long longValue = member.getLongAttribute("longKey");
        assertNotNull(longValue);
        assertEquals(Long.MAX_VALUE, longValue.longValue());

        Float floatValue = member.getFloatAttribute("floatKey");
        assertNotNull(floatValue);
        assertEquals(Float.MAX_VALUE, floatValue, 0.000001);

        Double doubleValue = member.getDoubleAttribute("doubleKey");
        assertNotNull(doubleValue);
        assertEquals(Double.MAX_VALUE, doubleValue, 0.000001);
    }

    @Test
    public void testConstructor_withMemberImpl() {
        MemberImpl member = new MemberImpl(new MemberImpl(address, VERSION));

        assertBasicMemberImplFields(member);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStringAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setStringAttribute("stringKey", "stringValue");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetBooleanAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setBooleanAttribute("booleanKeyTrue", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetByteAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setByteAttribute("byteKey", Byte.MAX_VALUE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShortAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setShortAttribute("shortKey", Short.MAX_VALUE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetIntAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setIntAttribute("intKey", Integer.MAX_VALUE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetLongAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setLongAttribute("longKey", Long.MAX_VALUE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetFloatAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setFloatAttribute("floatKey", Float.MAX_VALUE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetDoubleAttribute() {
        MemberImpl member = new MemberImpl(address, VERSION);
        member.setDoubleAttribute("doubleKey", Double.MAX_VALUE);
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
