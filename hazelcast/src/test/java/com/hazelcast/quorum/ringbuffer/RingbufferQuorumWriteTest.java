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

package com.hazelcast.quorum.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.ringbuffer.OverflowPolicy;
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

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferQuorumWriteTest extends AbstractQuorumTest {

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
    public void add_quorum() {
        ring(0).add("123");
    }

    @Test(expected = QuorumException.class)
    public void add_noQuorum() {
        ring(3).add("123");
    }

    @Test
    public void addAllAsync_quorum() throws Exception {
        ring(0).addAllAsync(asList("123"), OverflowPolicy.OVERWRITE).get();
    }

    @Test
    public void addAllAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        ring(3).addAllAsync(asList("123"), OverflowPolicy.OVERWRITE).get();
    }

    @Test
    public void addAsync_quorum() throws Exception {
        ring(0).addAsync("123", OverflowPolicy.OVERWRITE).get();
    }

    @Test
    public void addAsync_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        ring(3).addAsync("123", OverflowPolicy.OVERWRITE).get();
    }

    protected Ringbuffer ring(int index) {
        return ring(index, quorumType);
    }
}
