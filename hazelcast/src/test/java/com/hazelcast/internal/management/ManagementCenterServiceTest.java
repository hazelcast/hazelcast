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

package com.hazelcast.internal.management;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.management.ManagementCenterService.cleanupUrl;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ManagementCenterServiceTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testConstructor_withNullConfiguration() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        instance.getConfig().setManagementCenterConfig(null);
        new ManagementCenterService(getNode(instance).hazelcastInstance);
    }

    @Test
    public void testCleanupUrl() {
        String url = cleanupUrl("http://noCleanupNeeded/");
        assertTrue(url.endsWith("/"));
    }

    @Test
    public void testCleanupUrl_needsCleanup() {
        String url = cleanupUrl("http://needsCleanUp");
        assertTrue(url.endsWith("/"));
    }

    @Test
    public void testCleanupUrl_withNull() {
        assertNull(cleanupUrl(null));
    }
}
