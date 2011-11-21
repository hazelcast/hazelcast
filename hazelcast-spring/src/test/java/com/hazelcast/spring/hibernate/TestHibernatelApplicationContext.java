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

package com.hazelcast.spring.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.InetSocketAddress;
import java.util.Set;

import javax.annotation.Resource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"hibernate-applicationContext-hazelcast.xml"})
public class TestHibernatelApplicationContext {

    @Resource(name="instance")
    private HazelcastInstance instance;
    
    @Resource(name="cacheProvider")
    private HazelcastCacheProvider cacheProvider;
    
    @Resource(name="regionFactory")
    private HazelcastCacheRegionFactory regionFactory;
    
    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testInstance() {
        assertNotNull(instance);
        final Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        final Member member = members.iterator().next();
        final InetSocketAddress inetSocketAddress = member.getInetSocketAddress();
        assertEquals(5700, inetSocketAddress.getPort());
    }

    @Test
    public void testCacheProvider() {
        assertNotNull(cacheProvider);
        assertEquals(cacheProvider.getHazelcastInstance(), instance);
    }
    
    @Test
    public void testRegionFactory() {
        assertNotNull(regionFactory);
        assertEquals(regionFactory.getHazelcastInstance(), instance);
    }
}
