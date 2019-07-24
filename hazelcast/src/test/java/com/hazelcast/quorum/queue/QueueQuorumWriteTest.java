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

package com.hazelcast.quorum.queue;

import com.hazelcast.config.Config;
import com.hazelcast.collection.IQueue;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
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

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueQuorumWriteTest extends AbstractQuorumTest {

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
    public void put_quorum() throws Exception {
        queue(0).put("foo");
    }

    @Test(expected = QuorumException.class)
    public void put_noQuorum() throws Exception {
        queue(3).put("foo");
    }

    @Test
    public void offer_quorum() {
        queue(0).offer("foo");
    }

    @Test(expected = QuorumException.class)
    public void offer_noQuorum() {
        queue(3).offer("foo");
    }

    @Test
    public void add_quorum() {
        queue(0).add("foo");
    }

    @Test(expected = QuorumException.class)
    public void add_noQuorum() {
        queue(3).add("foo");
    }

    @Test
    public void take_quorum() throws Exception {
        queue(0).take();
    }

    @Test(expected = QuorumException.class)
    public void take_noQuorum() throws Exception {
        queue(3).take();
    }

    @Test
    public void remove_quorum() {
        queue(0).remove();
    }

    @Test(expected = QuorumException.class)
    public void remove_noQuorum() {
        queue(3).remove();
    }

    @Test
    public void poll_quorum() {
        queue(0).poll();
    }

    @Test(expected = QuorumException.class)
    public void poll_noQuorum() {
        queue(3).poll();
    }

    protected IQueue queue(int index) {
        return queue(index, quorumType);
    }
}
