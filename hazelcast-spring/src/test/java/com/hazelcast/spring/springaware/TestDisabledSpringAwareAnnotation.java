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

package com.hazelcast.spring.springaware;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertNull;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"springAware-disabled-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestDisabledSpringAwareAnnotation {

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    public void testDisabledSpringManagedContext() {
        HazelcastInstance instance = (HazelcastInstance) context.getBean("instance");
        assertNull(instance.getConfig().getManagedContext());

        HazelcastClientProxy client = (HazelcastClientProxy) context.getBean("client");
        assertNull(client.getClientConfig().getManagedContext());
    }
}
