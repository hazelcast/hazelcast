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

package com.hazelcast.impl;

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ClusterImplTest {
    @Test
    public void testDistance() throws Exception {
        List<Member> lsMembers = new ArrayList<Member>();
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 1), true, NodeType.MEMBER));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 2), true, NodeType.MEMBER));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 3), true, NodeType.SUPER_CLIENT));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(0), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(2), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), true));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), true));
        assertEquals(0, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), true));
        lsMembers.clear();
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 1), true, NodeType.MEMBER));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 2), true, NodeType.SUPER_CLIENT));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 3), true, NodeType.MEMBER));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(0), true));
        assertEquals(0, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(2), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), true));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), true));
        lsMembers.clear();
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 1), true, NodeType.SUPER_CLIENT));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 2), true, NodeType.MEMBER));
        lsMembers.add(new MemberImpl(new Address("1.1.1.1", 3), true, NodeType.MEMBER));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), false));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), false));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), false));
        assertEquals(0, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(1), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(0), lsMembers.get(2), true));
        assertEquals(2, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(0), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(1), lsMembers.get(2), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(0), true));
        assertEquals(1, ClusterImpl.calculateDistance(lsMembers, lsMembers.get(2), lsMembers.get(1), true));
    }
}
