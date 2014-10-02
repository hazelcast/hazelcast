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

package com.hazelcast.client.idgenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * @author ali 5/28/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientIdGeneratorTest {

    private final String name = "test";
    private HazelcastInstance hz;
    private IdGenerator i;

    @Before
    public void init() {
        Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        i = hz.getIdGenerator(name);
    }

    @After
    public void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testInit0_GivesId1() throws Exception {
        assertTrue( i.init(0) );
        assertEquals(1, i.newId());
    }

    @Test
    public void testGenerator() throws Exception {
        assertTrue(i.init(3569));
        assertFalse(i.init(4569));
        assertEquals(3570, i.newId());
    }

}
