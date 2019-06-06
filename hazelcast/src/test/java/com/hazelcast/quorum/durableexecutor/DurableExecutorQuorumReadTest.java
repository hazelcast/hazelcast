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

package com.hazelcast.quorum.durableexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.StaleTaskIdException;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorQuorumReadTest extends AbstractQuorumTest {

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
    public void isShutdown_quorum() {
        exec(0).isShutdown();
    }

    @Test
    public void isShutdown_noQuorum() {
        exec(3).isShutdown();
    }

    @Test
    public void isTerminated_quorum() {
        exec(0).isTerminated();
    }

    @Test
    public void isTerminated_noQuorum() {
        exec(3).isTerminated();
    }

    @Test
    public void awaitTermination_quorum() throws Exception {
        exec(0).awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void awaitTermination_noQuorum() throws Exception {
        exec(3).awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void retrieveResult_quorum() throws Exception {
        try {
            exec(0).retrieveResult(123L).get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StaleTaskIdException) {
                // expected & meaningless since not a real taskId
            }
        }
    }

    @Test
    public void retrieveResult_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).retrieveResult(125L).get();
    }

    protected DurableExecutorService exec(int index) {
        return durableExec(index, quorumType);
    }
}
