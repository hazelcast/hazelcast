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

package com.hazelcast.map;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.map.BackupExpirationTest.getTotalEntryCount;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class BackupExpirationBouncingMemberTest extends HazelcastTestSupport {

    String mapName = "test";
    int maxIdleSeconds = 2;
    int backupCount = 3;
    int keySpace = 1000;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .build();

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds)
                .setBackupCount(backupCount);
        return config;
    }

    @Test
    public void backups_should_be_empty_after_expiration() {
        Runnable[] methods = new Runnable[2];
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        methods[0] = new Get(testDriver);
        methods[1] = new Set(testDriver);

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                AtomicReferenceArray<HazelcastInstance> members = bounceMemberRule.getMembers();
                AtomicReferenceArray<HazelcastInstance> testDrivers = bounceMemberRule.getTestDrivers();

                assertSize(members);
                assertSize(testDrivers);
            }

            private void assertSize(AtomicReferenceArray<HazelcastInstance> members) {
                int length = members.length();
                for (int i = 0; i < length; i++) {
                    HazelcastInstance node = members.get(i);
                    assert node != null;
                    if (node.getLifecycleService().isRunning()
                            && node.getCluster().getClusterState() != ClusterState.PASSIVE) {

                        ClusterState clusterState = node.getCluster().getClusterState();
                        assertEquals("Current cluster state is:" + clusterState.toString(),
                                0, getTotalEntryCount(node.getMap(mapName)));
                    }
                }
            }
        });
    }

    private class Get implements Runnable {

        private final HazelcastInstance hz;

        public Get(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                hz.getMap(mapName).get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final HazelcastInstance hz;

        public Set(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                hz.getMap(mapName).set(i, i);
            }
        }
    }
}
