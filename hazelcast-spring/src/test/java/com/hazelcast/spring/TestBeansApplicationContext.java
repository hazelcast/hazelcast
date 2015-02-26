/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"beans-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestBeansApplicationContext {

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    public void test() {
        assertTrue(Hazelcast.getAllHazelcastInstances().isEmpty());
        assertTrue(HazelcastClient.getAllHazelcastClients().isEmpty());

        context.getBean("map2");

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

        HazelcastInstance hazelcast = Hazelcast.getAllHazelcastInstances().iterator().next();
        assertEquals(2, hazelcast.getDistributedObjects().size());

        context.getBean("client");
        context.getBean("client");
        assertEquals(3, HazelcastClient.getAllHazelcastClients().size());

        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertEquals(instance, Hazelcast.getAllHazelcastInstances().iterator().next());
    }

}
