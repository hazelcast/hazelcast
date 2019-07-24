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

package com.hazelcast.quorum.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "classLoaderType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void init_successful_whenQuorumSize_met() {
        semaphore(0).init(10);
    }

    @Test(expected = QuorumException.class)
    public void init_successful_whenQuorumSize_notMet() {
        semaphore(3).init(10);
    }

    @Test
    public void acquire_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).acquire();
    }

    @Test(expected = QuorumException.class)
    public void acquire_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).acquire();
    }

    @Test
    public void acquirePermits_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
        semaphore(0).acquire(2);
    }

    @Test(expected = QuorumException.class)
    public void acquirePermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).acquire(2);
    }

    @Test
    public void drainPermits_successful_whenQuorumSize_met() {
        int drained = 0;
        try {
            drained = semaphore(0).drainPermits();
        } finally {
            semaphore(0).release(drained);
        }
    }

    @Test(expected = QuorumException.class)
    public void drainPermits_successful_whenQuorumSize_notMet() {
        semaphore(3).drainPermits();
    }

    @Test
    public void reducePermits_successful_whenQuorumSize_met() {
        semaphore(0).release();
        semaphore(0).reducePermits(1);
    }

    @Test(expected = QuorumException.class)
    public void reducePermits_successful_whenQuorumSize_notMet() {
        semaphore(3).reducePermits(1);
    }

    @Test
    public void increase() {
        semaphore(0).drainPermits();
        semaphore(0).increasePermits(1);
    }

    @Test(expected = QuorumException.class)
    public void increasePermits_successful_whenQuorumSize_notMet() {
        semaphore(3).increasePermits(1);
    }

    @Test
    public void release_successful_whenQuorumSize_met() {
        semaphore(0).release();
    }

    @Test(expected = QuorumException.class)
    public void release_successful_whenQuorumSize_notMet() {
        semaphore(3).release();
    }

    @Test
    public void releasePermits_successful_whenQuorumSize_met() {
        semaphore(0).release(2);
    }

    @Test(expected = QuorumException.class)
    public void releasePermits_successful_whenQuorumSize_notMet() {
        semaphore(3).release(2);
    }

    @Test
    public void tryAcquire_successful_whenQuorumSize_met() {
        semaphore(0).release();
        semaphore(0).tryAcquire();
    }

    @Test(expected = QuorumException.class)
    public void tryAcquire_successful_whenQuorumSize_notMet() {
        semaphore(3).tryAcquire();
    }

    @Test
    public void tryAcquirePermits_successful_whenQuorumSize_met() {
        semaphore(0).release(2);
        semaphore(0).tryAcquire(2);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquirePermits_successful_whenQuorumSize_notMet() {
        semaphore(3).tryAcquire(2);
    }

    @Test
    public void tryAcquireTimeout_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).tryAcquire(10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquireTimeout_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire(10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryAcquirePermitsTimeout_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
        semaphore(0).tryAcquire(2, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquirePermitsTimeout_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire(2, 10, TimeUnit.MILLISECONDS);
    }

    protected ISemaphore semaphore(int index) {
        return semaphore(index, quorumType);
    }
}
