/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class LockQuorumReadTest extends AbstractLockQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.READ}, {QuorumType.READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void getRemainingLeaseTime_quorum() {
        lock(0).getRemainingLeaseTime();
    }

    @Test(expected = QuorumException.class)
    public void getRemainingLeaseTime_noQuorum() {
        lock(3).getRemainingLeaseTime();
    }

    @Test
    public void isLocked_quorum() {
        lock(0).isLocked();
    }

    @Test(expected = QuorumException.class)
    public void isLocked_noQuorum() {
        lock(3).isLocked();
    }

    @Test
    public void isLockedByCurrentThread_quorum() {
        lock(0).isLockedByCurrentThread();
    }

    @Test(expected = QuorumException.class)
    public void isLockedByCurrentThread_noQuorum() {
        lock(3).isLockedByCurrentThread();
    }

    @Test
    public void getLockCount_quorum() {
        lock(0).getLockCount();
    }

    @Test(expected = QuorumException.class)
    public void getLockCount_noQuorum() {
        lock(3).getLockCount();
    }

    protected ILock lock(int index) {
        return lock(index, quorumType);
    }
}
