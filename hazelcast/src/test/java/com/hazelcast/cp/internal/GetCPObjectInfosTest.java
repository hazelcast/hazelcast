/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPObjectInfo;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetCPObjectInfosTest extends HazelcastTestSupport {

    protected static TestHazelcastFactory factory;
    protected static HazelcastInstance[] instances;
    protected static HazelcastInstance client;

    protected static CPSubsystem cp;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setUpClass();
    }

    /** Doesn't set license to allow tests to override */
    protected static void setUpClass() {
        factory = new TestHazelcastFactory();

        Config config = new Config();
        // Group size intentionally smaller CP members, which is smaller than all members
        config.getCPSubsystemConfig()
                .setGroupSize(3)
                .setCPMemberCount(4);

        instances = factory.newInstances(config, 5);
        client = factory.newHazelcastClient();

        cp = instances[0].getCPSubsystem();

        cp.getAtomicLong("long1").set(1);
        cp.getAtomicLong("long2@my_group").set(1);

        cp.getAtomicLong("long3").set(1);
        cp.getAtomicLong("long3").destroy();
        cp.getAtomicLong("long4@my_group").set(1);
        cp.getAtomicLong("long4@my_group").destroy();

        cp.getAtomicReference("reference1").set("object");
        cp.getAtomicReference("reference2@my_group").set("object");

        cp.getAtomicReference("reference3").set("object");
        cp.getAtomicReference("reference3").destroy();
        cp.getAtomicReference("reference4@my_group").set("object");
        cp.getAtomicReference("reference4@my_group").destroy();

        cp.getCountDownLatch("countDownLatch1").trySetCount(10);
        cp.getCountDownLatch("countDownLatch2@my_group").trySetCount(10);

        cp.getCountDownLatch("countDownLatch3").trySetCount(10);
        cp.getCountDownLatch("countDownLatch3").destroy();
        cp.getCountDownLatch("countDownLatch4@my_group").trySetCount(10);
        cp.getCountDownLatch("countDownLatch4@my_group").destroy();

        cp.getSemaphore("semaphore1").init(1);
        cp.getSemaphore("semaphore2@my_group").init(1);

        cp.getSemaphore("semaphore3").init(1);
        cp.getSemaphore("semaphore3").destroy();
        cp.getSemaphore("semaphore4@my_group").init(1);
        cp.getSemaphore("semaphore4@my_group").destroy();

        cp.getLock("lock1").tryLock();
        cp.getLock("lock2@my_group").tryLock();

        cp.getLock("lock3").tryLock();
        cp.getLock("lock3").unlock();
        cp.getLock("lock3").destroy();
        cp.getLock("lock4@my_group").tryLock();
        cp.getLock("lock4@my_group").unlock();
        cp.getLock("lock4@my_group").destroy();

        cp.getMap("map1").put(1, 1);
        cp.getMap("map2@my_group").put(1, 1);

        cp.getMap("map3").put(1, 1);
        cp.getMap("map3").destroy();

        cp.getMap("map4@my_group").put(1, 1);
        cp.getMap("map4@my_group").destroy();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void getCPObjectInfos_atomicLong() {
        assertGroupObjectInfo("default", AtomicLongService.SERVICE_NAME, "long1");
        assertGroupObjectInfo("my_group", AtomicLongService.SERVICE_NAME, "long2");
    }

    @Test
    public void getCPTombstoneInfos_atomicLong() {
        assertTombstoneObject("default", AtomicLongService.SERVICE_NAME, "long3");

        assertTombstoneObject("my_group", AtomicLongService.SERVICE_NAME, "long4");
    }

    @Test
    public void getCPObjectInfos_atomicReference() {
        assertGroupObjectInfo("default", AtomicRefService.SERVICE_NAME, "reference1");
        assertGroupObjectInfo("my_group", AtomicRefService.SERVICE_NAME, "reference2");
    }

    @Test
    public void getCPTombstoneInfos_atomicReference() {
        assertTombstoneObject("default", AtomicRefService.SERVICE_NAME, "reference3");
        assertTombstoneObject("my_group", AtomicRefService.SERVICE_NAME, "reference4");
    }

    @Test
    public void getCPObjectInfos_countDownLatch() {
        assertGroupObjectInfo("default", CountDownLatchService.SERVICE_NAME, "countDownLatch1");
        assertGroupObjectInfo("my_group", CountDownLatchService.SERVICE_NAME, "countDownLatch2");
    }

    @Test
    public void getCPTombstoneInfos_countDownLatch() {
        assertTombstoneObject("default", CountDownLatchService.SERVICE_NAME, "countDownLatch3");
        assertTombstoneObject("my_group", CountDownLatchService.SERVICE_NAME, "countDownLatch4");
    }

    @Test
    public void getCPObjectInfos_semaphore() {
        assertGroupObjectInfo("default", SemaphoreService.SERVICE_NAME, "semaphore1");
        assertGroupObjectInfo("my_group", SemaphoreService.SERVICE_NAME, "semaphore2");
    }

    @Test
    public void getCPTombstoneInfos_semaphore() {
        assertTombstoneObject("default", SemaphoreService.SERVICE_NAME, "semaphore3");
        assertTombstoneObject("my_group", SemaphoreService.SERVICE_NAME, "semaphore4");
    }

    @Test
    public void getCPObjectInfos_lock() {
        assertGroupObjectInfo("default", LockService.SERVICE_NAME, "lock1");
        assertGroupObjectInfo("my_group", LockService.SERVICE_NAME, "lock2");
    }

    @Test
    public void getCPTombstoneInfos_lock() {
        assertTombstoneObject("default", LockService.SERVICE_NAME, "lock3");
        assertTombstoneObject("my_group", LockService.SERVICE_NAME, "lock4");
    }

    @Test
    public void testGetCPObjectInfosCPMap() {
        assertGroupObjectInfo("default", CPSubsystem.CP_MAP, "map1");
        assertGroupObjectInfo("my_group", CPSubsystem.CP_MAP, "map2");
    }

    @Test
    public void testGetCPTombstoneInfosCPMap() {
        assertTombstoneObject("default", CPSubsystem.CP_MAP, "map3");
        assertTombstoneObject("my_group", CPSubsystem.CP_MAP, "map4");
    }

    @Test
    public void getCPObjectInfos_atomicLong_all_members() {
        CPGroupId defaultGroupId = groupId("default");
        for (HazelcastInstance member : instances) {
            CPSubsystem memberCP = member.getCPSubsystem();

            Iterable<CPObjectInfo> defaultGroupInfos = memberCP.getObjectInfos(defaultGroupId, AtomicLongService.SERVICE_NAME);
            assertThat(defaultGroupInfos)
                    .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                    .containsExactly(tuple("long1", AtomicLongService.SERVICE_NAME, defaultGroupId));
        }
    }

    protected CPGroupId groupId(String name) {
        try {
            return instances[0].getCPSubsystem()
                    .getCPSubsystemManagementService()
                    .getCPGroup(name)
                    .toCompletableFuture()
                    .get().id();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    void assertGroupObjectInfo(String groupName, String serviceName, String objectName) {
        CPGroupId groupId = groupId(groupName);
        Iterable<CPObjectInfo> myGroupInfos = cp.getObjectInfos(groupId, serviceName);
        assertThat(myGroupInfos)
                .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                .containsExactly(tuple(objectName, serviceName, groupId));
    }

    void assertTombstoneObject(String groupName, String serviceName, String objectName) {
        CPGroupId groupId = groupId(groupName);
        Iterable<CPObjectInfo> myGroupInfos = cp.getTombstoneInfos(groupId, serviceName);
        assertThat(myGroupInfos)
                .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                .containsExactly(tuple(objectName, serviceName, groupId));
    }

}
