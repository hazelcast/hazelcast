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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.durableexecutor.DurableExecutorQuorumWriteTest.ExecRunnable.callable;
import static com.hazelcast.quorum.durableexecutor.DurableExecutorQuorumWriteTest.ExecRunnable.runnable;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyOwnedBy;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class DurableExecutorQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
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
    public void disposeResult_quorum() {
        try {
            exec(0).disposeResult(123L);
        } catch (StaleTaskIdException ex) {
            // expected & meaningless since not a real taskId
        }
    }

    @Test
    public void disposeResult_noQuorum() {
        expectedException.expect(isA(QuorumException.class));
        exec(3).disposeResult(125L);
    }

    @Test
    public void retrieveAndDisposeResult_quorum() throws Exception {
        try {
            exec(0).retrieveAndDisposeResult(123L).get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StaleTaskIdException) {
                // expected & meaningless since not a real taskId
            }
        }
    }

    @Test
    public void retrieveAndDisposeResult_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).retrieveAndDisposeResult(125L).get();
    }


    @Test
    public void executeOnKeyOwner_quorum() {
        exec(0).executeOnKeyOwner(runnable(), key(0));
    }

    @Test
    public void executeOnKeyOwner_noQuorum() {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnKeyOwner(runnable(), key(3));
    }

    @Test
    public void execute_quorum() {
        exec(0).execute(runnable());
    }

    @Test
    public void execute_noQuorum() {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).execute(runnable());
    }

    @Test
    public void submit_runnable_quorum() throws Exception {
        exec(0).submit(runnable()).get();
    }

    @Test
    public void submit_runnable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submit(runnable()).get();
    }

    @Test
    public void submit_runnable_result_quorum() throws Exception {
        exec(0).submit(runnable(), "result").get();
    }

    @Test
    public void submit_runnable_result_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submit(runnable(), "result").get();
    }

    @Test
    public void submit_callable_quorum() throws Exception {
        exec(0).submit(callable()).get();
    }

    @Test
    public void submit_callable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submit(callable()).get();
    }

    @Test
    public void submitToKeyOwner_callable_quorum() throws Exception {
        exec(0).submitToKeyOwner(callable(), key(0)).get();
    }

    @Test
    public void submitToKeyOwner_callable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submitToKeyOwner(callable(), key(3)).get();
    }

    @Test
    public void submitToKeyOwner_runnable_quorum() throws Exception {
        exec(0).submitToKeyOwner(runnable(), key(0)).get();
    }

    @Test
    public void submitToKeyOwner_runnable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submitToKeyOwner(runnable(), key(3)).get();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_quorum() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_noQuorum() throws Exception {
        wait(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_quorum() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_noQuorum() throws Exception {
        wait(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_quorum() throws Exception {
        exec(0).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_noQuorum() throws Exception {
        exec(3).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_timeout_quorum() throws Exception {
        exec(0).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_timeout_noQuorum() throws Exception {
        exec(3).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS);
    }

    @Test
    public void shutdown_quorum() {
        exec(0, "shutdown").shutdown();
    }

    @Test(expected = QuorumException.class)
    public void shutdown_noQuorum() {
        exec(3, "shutdown").shutdown();
    }

    @Test
    public void shutdownNow_quorum() {
        exec(0, "shutdownNow").shutdownNow();
    }

    @Test(expected = QuorumException.class)
    public void shutdownNow_noQuorum() {
        exec(3, "shutdownNow").shutdownNow();
    }

    protected DurableExecutorService exec(int index) {
        return durableExec(index, quorumType);
    }

    protected DurableExecutorService exec(int index, String postfix) {
        return durableExec(index, quorumType, postfix);
    }

    private Object key(int index) {
        return generateKeyOwnedBy(cluster.getInstance(index), true);
    }

    private void wait(Collection<? extends Future<?>> futures) throws ExecutionException, InterruptedException {
        for (Future<?> f : futures) {
            f.get();
        }
    }

    static class ExecRunnable implements Runnable, Callable, Serializable {

        @Override
        public Object call() throws Exception {
            return "response";
        }

        public void run() {
        }

        public static Runnable runnable() {
            return new ExecRunnable();
        }

        public static Callable callable() {
            return new ExecRunnable();
        }
    }

}
