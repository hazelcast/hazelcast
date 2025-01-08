/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.hibernate;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.HazelcastLocalCacheRegionFactory;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"hibernate-applicationContext-hazelcast.xml"})
class TestHibernateApplicationContext {

    @Autowired
    private HazelcastInstance instance;

    @Autowired
    private HazelcastCacheRegionFactory regionFactory;

    @Autowired
    private HazelcastLocalCacheRegionFactory localRegionFactory;

    @Autowired
    private HazelcastLocalCacheRegionFactory localRegionFactory2;

    @BeforeAll
    @AfterAll
    static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    void testInstance() {
        assertNotNull(instance);
        Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        Member member = members.iterator().next();
        InetSocketAddress inetSocketAddress = member.getSocketAddress();
        assertEquals(5700, inetSocketAddress.getPort());
    }

    @Test
    void testRegionFactory() {
        assertNotNull(regionFactory);
        assertEquals(regionFactory.getHazelcastInstance(), instance);

        assertNotNull(localRegionFactory);
        assertEquals(localRegionFactory.getHazelcastInstance(), instance);

        assertNotNull(localRegionFactory2);
        assertEquals(localRegionFactory2.getHazelcastInstance(), instance);
    }
}
