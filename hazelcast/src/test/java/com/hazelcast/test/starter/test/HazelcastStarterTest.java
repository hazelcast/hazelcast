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

package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class HazelcastStarterTest {

    @Test
    public void testMember() throws InterruptedException {
        HazelcastInstance alwaysRunningMember = HazelcastStarter.newHazelcastInstance("3.7", false);

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting member " + version);
            HazelcastInstance instance = HazelcastStarter.newHazelcastInstance(version);
            System.out.println("Stopping member " + version);
            instance.shutdown();
        }

        alwaysRunningMember.shutdown();
    }

    @Test
    public void testMemberWithConfig() throws InterruptedException {
        Config config = new Config();
        config.setInstanceName("test-name");

        HazelcastInstance alwaysRunningMember = HazelcastStarter.newHazelcastInstance("3.8", config, false);

        assertEquals(alwaysRunningMember.getName(), "test-name");
        alwaysRunningMember.shutdown();
    }


}
