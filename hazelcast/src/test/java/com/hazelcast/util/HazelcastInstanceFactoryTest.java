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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class HazelcastInstanceFactoryTest extends HazelcastTestSupport {

    @Test
    public void testTestHazelcastInstanceFactory() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());
            }
        });

        instanceFactory.terminateAll();

    }

    @Test
    public void testTestHazelcastInstanceFactory_withTwoFactories() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        TestHazelcastInstanceFactory instanceFactory2 = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1_2 = instanceFactory2.newHazelcastInstance();
        final HazelcastInstance instance2_2 = instanceFactory2.newHazelcastInstance();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, instance1_2.getCluster().getMembers().size());
                assertEquals(2, instance2_2.getCluster().getMembers().size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());
            }
        });

        instanceFactory.terminateAll();
        instanceFactory2.terminateAll();

    }
}
