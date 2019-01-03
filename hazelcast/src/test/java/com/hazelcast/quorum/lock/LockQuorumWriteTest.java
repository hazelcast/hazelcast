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

package com.hazelcast.quorum.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.ILock;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void tryLock_quorum() {
        lock(0).tryLock();
    }

    @Test(expected = QuorumException.class)
    public void tryLock_noQuorum() {
        lock(3).tryLock();
    }

    @Test
    public void tryLockTimeout_quorum() throws InterruptedException {
        lock(0).tryLock(5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryLockTimeout_noQuorum() throws InterruptedException {
        lock(3).tryLock(1, TimeUnit.MINUTES);
    }

    @Test
    public void tryLockTimeoutLeaseTime_quorum() throws InterruptedException {
        lock(0).tryLock(5, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryLockTimeoutLeaseTime_noQuorum() throws InterruptedException {
        lock(3).tryLock(1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
    }

    @Test
    public void lock_quorum() {
        lock(0).forceUnlock();
        lock(0).lock();
    }

    @Test(expected = QuorumException.class)
    public void lock_noQuorum() {
        lock(3).lock();
    }

    @Test
    public void lockLeaseTime_quorum() {
        lock(0).forceUnlock();
        lock(0).lock(1, TimeUnit.MINUTES);
    }

    @Test(expected = QuorumException.class)
    public void lockLeaseTime_noQuorum() {
        lock(3).lock(1, TimeUnit.MINUTES);
    }

    @Test
    public void forceUnlock_quorum() {
        lock(0).forceUnlock();
    }

    @Test(expected = QuorumException.class)
    public void forceUnlock_noQuorum() {
        lock(4).forceUnlock();
    }

    @Test
    public void unlock_quorum() {
        lock(0).forceUnlock();
        lock(0).lock();

        lock(0).unlock();
    }

    @Test(expected = QuorumException.class)
    public void unlock_noQuorum() {
        lock(3).unlock();
    }

    @Test(expected = QuorumException.class)
    public void newCondition_noQuorum() throws Exception {
        lock(3).newCondition("condition").await();
    }

    @Test
    public void newConditionSignal_quorum() {
        lock(0).forceUnlock();
        lock(0).lock();

        lock(0).newCondition("condition").signal();
    }

    @Test(expected = QuorumException.class)
    public void newConditionSignal_noQuorum() {
        lock(3).newCondition("condition").signal();
    }

    protected ILock lock(int index) {
        return lock(index, quorumType);
    }
}
