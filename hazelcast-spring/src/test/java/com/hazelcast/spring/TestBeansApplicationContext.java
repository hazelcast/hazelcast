/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"beans-applicationContext-hazelcast.xml"})
public class TestBeansApplicationContext {

    @BeforeClass
    @AfterClass
    public static void start() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    public void testLazy() {
        Assert.assertTrue(Hazelcast.getAllHazelcastInstances().isEmpty());
        Assert.assertTrue(HazelcastClient.getAllHazelcastClients().isEmpty());

        context.getBean("map2");

        Assert.assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        Assert.assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

        HazelcastInstance hazelcast = Hazelcast.getAllHazelcastInstances().iterator().next();
        Assert.assertEquals(2, hazelcast.getDistributedObjects().size());
    }

    @Test
    public void testScope() {
        context.getBean("client");
        context.getBean("client");
        Assert.assertEquals(3, HazelcastClient.getAllHazelcastClients().size());

        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        Assert.assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        Assert.assertEquals(instance, Hazelcast.getAllHazelcastInstances().iterator().next());
    }

}
