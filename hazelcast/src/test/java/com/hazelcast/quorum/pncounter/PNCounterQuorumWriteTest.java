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

package com.hazelcast.quorum.pncounter;

import com.hazelcast.config.Config;
import com.hazelcast.crdt.pncounter.PNCounter;
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
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterQuorumWriteTest extends AbstractQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void addAndGet_quorum() {
        pnCounter(0).addAndGet(1);
    }

    @Test(expected = QuorumException.class)
    public void addAndGet_noQuorum() {
        pnCounter(3).addAndGet(1);
    }

    @Test
    public void getAndAdd_quorum() {
        pnCounter(0).getAndAdd(1);
    }

    @Test(expected = QuorumException.class)
    public void getAndAdd_noQuorum() {
        pnCounter(3).getAndAdd(1);
    }

    @Test
    public void incrementAndGet_quorum() {
        pnCounter(0).incrementAndGet();
    }

    @Test(expected = QuorumException.class)
    public void incrementAndGet_noQuorum() {
        pnCounter(3).incrementAndGet();
    }

    @Test
    public void getAndIncrement_quorum() {
        pnCounter(0).getAndIncrement();
    }

    @Test(expected = QuorumException.class)
    public void getAndIncrement_noQuorum() {
        pnCounter(3).getAndIncrement();
    }

    @Test
    public void subtractAndGet_quorum() {
        pnCounter(0).subtractAndGet(1);
    }

    @Test(expected = QuorumException.class)
    public void subtractAndGet_noQuorum() {
        pnCounter(3).subtractAndGet(1);
    }

    @Test
    public void getAndSubtract_quorum() {
        pnCounter(0).getAndSubtract(1);
    }

    @Test(expected = QuorumException.class)
    public void getAndSubtract_noQuorum() {
        pnCounter(3).getAndSubtract(1);
    }

    @Test
    public void decrementAndGet_quorum() {
        pnCounter(0).decrementAndGet();
    }

    @Test(expected = QuorumException.class)
    public void decrementAndGet_noQuorum() {
        pnCounter(3).decrementAndGet();
    }

    @Test
    public void getAndDecrement_quorum() {
        pnCounter(0).getAndDecrement();
    }

    @Test(expected = QuorumException.class)
    public void getAndDecrement_noQuorum() {
        pnCounter(3).getAndDecrement();
    }

    protected PNCounter pnCounter(int index) {
        return pnCounter(index, quorumType);
    }

}
