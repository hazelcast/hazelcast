/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.extensions.replicatedmap;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorTest {
    @org.junit.Test
    public void testEqual() throws Exception {
        Member m1 = new MemberImpl();
        Member m2 = new MemberImpl();
        Member m3 = new MemberImpl();
        
        Vector a = new Vector();
        Vector b = new Vector();
        a.map.put(m1, new AtomicInteger(1));
        a.map.put(m2, new AtomicInteger(1));
        a.map.put(m3, new AtomicInteger(1));
        
        b.map.put(m1, new AtomicInteger(1));
        b.map.put(m2, new AtomicInteger(1));
        b.map.put(m3, new AtomicInteger(1));
        
        assertTrue(a.desc(b));
    }

    @org.junit.Test
    public void testDesc() throws Exception {
        Member m1 = new MemberImpl();
        Member m2 = new MemberImpl();
        Member m3 = new MemberImpl();

        Vector a = new Vector();
        Vector b = new Vector();
        a.map.put(m1, new AtomicInteger(0));
        a.map.put(m2, new AtomicInteger(1));
        a.map.put(m3, new AtomicInteger(0));

        b.map.put(m1, new AtomicInteger(0));
        b.map.put(m2, new AtomicInteger(0));
        b.map.put(m3, new AtomicInteger(0));

        assertTrue(a.desc(b));
    }
    @org.junit.Test
    public void testNotDesc() throws Exception {
        Member m1 = new MemberImpl();
        Member m2 = new MemberImpl();
        Member m3 = new MemberImpl();

        Vector a = new Vector();
        Vector b = new Vector();
        a.map.put(m1, new AtomicInteger(0));
        a.map.put(m2, new AtomicInteger(1));
        a.map.put(m3, new AtomicInteger(0));

        b.map.put(m1, new AtomicInteger(0));
        b.map.put(m2, new AtomicInteger(1));
        b.map.put(m3, new AtomicInteger(1));

        assertFalse(a.desc(b));
    }

    @org.junit.Test
    public void testWhenAbsent() throws Exception {
        Member m1 = new MemberImpl();
        Member m2 = new MemberImpl();
        Member m3 = new MemberImpl();

        Vector a = new Vector();
        a.map = new IdentityHashMap<Member, AtomicInteger>();
        Vector b = new Vector();
        b.map = new IdentityHashMap<Member, AtomicInteger>();

        a.map.put(m1, new AtomicInteger(1));
        a.map.put(m2, new AtomicInteger(1));

        b.map.put(m1, new AtomicInteger(1));
        b.map.put(m2, new AtomicInteger(2));
        b.map.put(m3, new AtomicInteger(1));

        assertFalse(a.desc(b));
    }
}
