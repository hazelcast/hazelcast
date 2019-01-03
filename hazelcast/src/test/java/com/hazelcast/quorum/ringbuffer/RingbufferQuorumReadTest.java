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

package com.hazelcast.quorum.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.core.IFunction;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.ringbuffer.Ringbuffer;
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

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferQuorumReadTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.READ}, {QuorumType.READ_WRITE}});
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
    public void capacity_quorum() {
        ring(0).capacity();
    }

    @Test(expected = QuorumException.class)
    public void capacity_noQuorum() {
        ring(3).capacity();
    }

    @Test
    public void size_quorum() {
        ring(0).size();
    }

    @Test(expected = QuorumException.class)
    public void size_noQuorum() {
        ring(3).size();
    }

    @Test
    public void tailSequence_quorum() {
        ring(0).tailSequence();
    }

    @Test(expected = QuorumException.class)
    public void tailSequence_noQuorum() {
        ring(3).tailSequence();
    }

    @Test
    public void headSequence_quorum() {
        ring(0).headSequence();
    }

    @Test(expected = QuorumException.class)
    public void headSequence_noQuorum() {
        ring(3).headSequence();
    }

    @Test
    public void remainingCapacity_quorum() {
        ring(0).remainingCapacity();
    }

    @Test(expected = QuorumException.class)
    public void remainingCapacity_noQuorum() {
        ring(3).remainingCapacity();
    }

    @Test
    public void readOne_quorum() throws InterruptedException {
        ring(0).readOne(1L);
    }

    @Test(expected = QuorumException.class)
    public void readOne_noQuorum() throws InterruptedException {
        ring(3).readOne(1L);
    }

    @Test
    public void readManyAsync_quorum() throws Exception {
        ring(0).readManyAsync(1L, 1, 1, new Filter()).get();
    }

    @Test(expected = QuorumException.class)
    public void readManyAsync_noQuorum() throws Throwable {
        try {
            ring(3).readManyAsync(1L, 1, 1, new Filter()).get();
        } catch (Exception ex) {
            throw ex.getCause();
        }
    }

    private static class Filter implements IFunction {
        @Override
        public Object apply(Object input) {
            return true;
        }
    }

    protected Ringbuffer ring(int index) {
        return ring(index, quorumType);
    }
}
