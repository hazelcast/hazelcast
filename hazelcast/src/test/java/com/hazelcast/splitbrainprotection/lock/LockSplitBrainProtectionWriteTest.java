/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.splitbrainprotection.lock;

import com.hazelcast.cp.lock.ILock;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void tryLock_splitBrainProtection() {
        lock(0).tryLock();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void tryLock_noSplitBrainProtection() {
        lock(3).tryLock();
    }

    @Test
    public void tryLockTimeout_splitBrainProtection() throws InterruptedException {
        lock(0).tryLock(5, TimeUnit.SECONDS);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void tryLockTimeout_noSplitBrainProtection() throws InterruptedException {
        lock(3).tryLock(1, TimeUnit.MINUTES);
    }

    @Test
    public void tryLockTimeoutLeaseTime_splitBrainProtection() throws InterruptedException {
        lock(0).tryLock(5, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void tryLockTimeoutLeaseTime_noSplitBrainProtection() throws InterruptedException {
        lock(3).tryLock(1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
    }

    @Test
    public void lock_splitBrainProtection() {
        lock(0).forceUnlock();
        lock(0).lock();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void lock_noSplitBrainProtection() {
        lock(3).lock();
    }

    @Test
    public void lockLeaseTime_splitBrainProtection() {
        lock(0).forceUnlock();
        lock(0).lock(1, TimeUnit.MINUTES);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void lockLeaseTime_noSplitBrainProtection() {
        lock(3).lock(1, TimeUnit.MINUTES);
    }

    @Test
    public void forceUnlock_splitBrainProtection() {
        lock(0).forceUnlock();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void forceUnlock_noSplitBrainProtection() {
        lock(4).forceUnlock();
    }

    @Test
    public void unlock_splitBrainProtection() {
        lock(0).forceUnlock();
        lock(0).lock();

        lock(0).unlock();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void unlock_noSplitBrainProtection() {
        lock(3).unlock();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void newCondition_noSplitBrainProtection() throws Exception {
        lock(3).newCondition("condition").await();
    }

    @Test
    public void newConditionSignal_splitBrainProtection() {
        lock(0).forceUnlock();
        lock(0).lock();

        lock(0).newCondition("condition").signal();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void newConditionSignal_noSplitBrainProtection() {
        lock(3).newCondition("condition").signal();
    }

    protected ILock lock(int index) {
        return lock(index, splitBrainProtectionOn);
    }
}
