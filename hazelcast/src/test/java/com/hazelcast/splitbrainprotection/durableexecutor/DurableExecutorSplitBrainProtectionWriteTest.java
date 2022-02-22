/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.splitbrainprotection.durableexecutor;

import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParametrizedRunner;
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

import static com.hazelcast.splitbrainprotection.durableexecutor.DurableExecutorSplitBrainProtectionWriteTest.ExecRunnable.callable;
import static com.hazelcast.splitbrainprotection.durableexecutor.DurableExecutorSplitBrainProtectionWriteTest.ExecRunnable.runnable;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.WRITE}, {SplitBrainProtectionOn.READ_WRITE}});
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
    public void disposeResult_splitBrainProtection() {
        try {
            exec(0).disposeResult(123L);
        } catch (StaleTaskIdException ex) {
            // expected & meaningless since not a real taskId
        }
    }

    @Test
    public void disposeResult_noSplitBrainProtection() {
        expectedException.expect(isA(SplitBrainProtectionException.class));
        exec(3).disposeResult(125L);
    }

    @Test
    public void retrieveAndDisposeResult_splitBrainProtection() throws Exception {
        try {
            exec(0).retrieveAndDisposeResult(123L).get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StaleTaskIdException) {
                // expected & meaningless since not a real taskId
            }
        }
    }

    @Test
    public void retrieveAndDisposeResult_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).retrieveAndDisposeResult(125L).get();
    }


    @Test
    public void executeOnKeyOwner_splitBrainProtection() {
        exec(0).executeOnKeyOwner(runnable(), key(0));
    }

    @Test
    public void executeOnKeyOwner_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).executeOnKeyOwner(runnable(), key(3));
    }

    @Test
    public void execute_splitBrainProtection() {
        exec(0).execute(runnable());
    }

    @Test
    public void execute_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).execute(runnable());
    }

    @Test
    public void submit_runnable_splitBrainProtection() throws Exception {
        exec(0).submit(runnable()).get();
    }

    @Test
    public void submit_runnable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submit(runnable()).get();
    }

    @Test
    public void submit_runnable_result_splitBrainProtection() throws Exception {
        exec(0).submit(runnable(), "result").get();
    }

    @Test
    public void submit_runnable_result_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submit(runnable(), "result").get();
    }

    @Test
    public void submit_callable_splitBrainProtection() throws Exception {
        exec(0).submit(callable()).get();
    }

    @Test
    public void submit_callable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submit(callable()).get();
    }

    @Test
    public void submitToKeyOwner_callable_splitBrainProtection() throws Exception {
        exec(0).submitToKeyOwner(callable(), key(0)).get();
    }

    @Test
    public void submitToKeyOwner_callable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submitToKeyOwner(callable(), key(3)).get();
    }

    @Test
    public void submitToKeyOwner_runnable_splitBrainProtection() throws Exception {
        exec(0).submitToKeyOwner(runnable(), key(0)).get();
    }

    @Test
    public void submitToKeyOwner_runnable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submitToKeyOwner(runnable(), key(3)).get();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_splitBrainProtection() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_noSplitBrainProtection() throws Exception {
        wait(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_splitBrainProtection() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_noSplitBrainProtection() throws Exception {
        wait(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_splitBrainProtection() throws Exception {
        exec(0).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_noSplitBrainProtection() throws Exception {
        exec(3).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_timeout_splitBrainProtection() throws Exception {
        exec(0).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_timeout_noSplitBrainProtection() throws Exception {
        exec(3).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS);
    }

    @Test
    public void shutdown_splitBrainProtection() {
        exec(0, "shutdown").shutdown();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void shutdown_noSplitBrainProtection() {
        exec(3, "shutdown").shutdown();
    }

    @Test
    public void shutdownNow_splitBrainProtection() {
        exec(0, "shutdownNow").shutdownNow();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void shutdownNow_noSplitBrainProtection() {
        exec(3, "shutdownNow").shutdownNow();
    }

    protected DurableExecutorService exec(int index) {
        return durableExec(index, splitBrainProtectionOn);
    }

    protected DurableExecutorService exec(int index, String postfix) {
        return durableExec(index, splitBrainProtectionOn, postfix);
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
