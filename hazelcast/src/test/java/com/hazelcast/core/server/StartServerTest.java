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

package com.hazelcast.core.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StartServerTest extends HazelcastTestSupport {

    private File parent = new File("ports");
    private File child = new File(parent, "hz.ports");

    @Before
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void setUp() {
        parent.mkdir();
    }

    @After
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void tearDown() {
        child.delete();
        parent.delete();

        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(StartServer.class);
    }

    @Test
    public void testMain() throws Exception {
        System.setProperty("print.port", child.getName());

        StartServer.main(new String[]{});

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertTrue(child.exists());
    }
}
