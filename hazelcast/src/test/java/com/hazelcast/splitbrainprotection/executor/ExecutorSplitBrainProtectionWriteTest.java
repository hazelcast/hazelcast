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

package com.hazelcast.splitbrainprotection.executor;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.splitbrainprotection.executor.ExecutorSplitBrainProtectionWriteTest.Callback.callback;
import static com.hazelcast.splitbrainprotection.executor.ExecutorSplitBrainProtectionWriteTest.ExecRunnable.callable;
import static com.hazelcast.splitbrainprotection.executor.ExecutorSplitBrainProtectionWriteTest.ExecRunnable.runnable;
import static com.hazelcast.splitbrainprotection.executor.ExecutorSplitBrainProtectionWriteTest.MultiCallback.multiCallback;
import static com.hazelcast.splitbrainprotection.executor.ExecutorSplitBrainProtectionWriteTest.Selector.selector;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

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
    public void executeOnAllMembers_splitBrainProtection() {
        exec(0).executeOnAllMembers(runnable());
    }

    @Test
    public void executeOnAllMembers_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).executeOnAllMembers(runnable());
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
    public void executeOnMember_splitBrainProtection() {
        exec(0).executeOnMember(runnable(), member(0));
    }

    @Test
    public void executeOnMember_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).executeOnMember(runnable(), member(3));
    }

    @Test
    public void executeOnMembers_collection_splitBrainProtection() {
        exec(0).executeOnMembers(runnable(), asList(member(0)));
    }

    @Test
    public void executeOnMembers_collection_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).executeOnMembers(runnable(), asList(member(3)));
    }

    @Test
    public void executeOnMembers_selector_splitBrainProtection() {
        exec(0).executeOnMembers(runnable(), selector(0));
    }

    @Test
    public void executeOnMembers_selector_noSplitBrainProtection() {
        // fire and forget operation, no split brain protection exception propagation
        // expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).executeOnMembers(runnable(), selector(3));
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
    public void submit_runnable_selector_splitBrainProtection() throws Exception {
        exec(0).submit(runnable(), selector(0)).get();
    }

    @Test
    public void submit_runnable_selector_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submit(runnable(), selector(3)).get();
    }

    @Test
    public void submit_runnable_selector_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submit(runnable(), selector(0), callback);
        callback.get();
    }

    @Test
    public void submit_runnable_selector_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submit(runnable(), selector(3), callback());
        expectSplitBrainProtectionException(callback);
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
    public void submit_callable_selector_splitBrainProtection() throws Exception {
        exec(0).submit(callable(), selector(0)).get();
    }

    @Test
    public void submit_callable_selector_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submit(callable(), selector(3)).get();
    }

    @Test
    public void submit_callable_selector_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submit(callable(), selector(0), callback);
        callback.get();
    }

    @Test
    public void submit_callable_selector_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submit(callable(), selector(3), callback());
        expectSplitBrainProtectionException(callback);
    }

    @Test
    public void submitToAllMembers_callable_splitBrainProtection() throws Exception {
        wait(exec(0).submitToAllMembers(callable()));
    }

    @Test
    public void submitToAllMembers_callable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        wait(exec(3).submitToAllMembers(callable()));
    }

    @Test
    public void submitToAllMembers_callable_multiCallback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToAllMembers(callable(), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToAllMembers_callable_multiCallback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToAllMembers(callable(), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
    }

    @Test
    public void submitToAllMembers_runnable_multiCallback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToAllMembers(runnable(), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToAllMembers_runnable_multiCallback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToAllMembers(runnable(), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
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
    public void submitToKeyOwner_runnable_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submitToKeyOwner(runnable(), key(0), callback);
        callback.get();
    }

    @Test
    public void submitToKeyOwner_runnable_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submitToKeyOwner(runnable(), key(3), callback);
        expectSplitBrainProtectionException(callback);
    }

    @Test
    public void submitToKeyOwner_callable_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submitToKeyOwner(callable(), key(0), callback);
        callback.get();
    }

    @Test
    public void submitToKeyOwner_callable_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submitToKeyOwner(callable(), key(3), callback);
        expectSplitBrainProtectionException(callback);
    }

    @Test
    public void submitToMember_callable_splitBrainProtection() throws Exception {
        exec(0).submitToMember(callable(), member(0)).get();
    }

    @Test
    public void submitToMember_callable_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        exec(3).submitToMember(callable(), member(3)).get();
    }

    @Test
    public void submitToMember_runnable_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submitToMember(runnable(), member(0), callback);
        callback.get();
    }

    @Test
    public void submitToMember_runnable_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submitToMember(runnable(), member(3), callback);
        expectSplitBrainProtectionException(callback);
    }

    @Test
    public void submitToMember_callable_callback_splitBrainProtection() {
        Callback callback = callback();
        exec(0).submitToMember(callable(), member(0), callback);
        callback.get();
    }

    @Test
    public void submitToMember_callable_callback_noSplitBrainProtection() {
        Callback callback = callback();
        exec(3).submitToMember(callable(), member(3), callback);
        expectSplitBrainProtectionException(callback);
    }

    @Test
    public void submitToMembers_callable_member_splitBrainProtection() throws Exception {
        wait(exec(0).submitToMembers(callable(), asList(member(0))));
    }

    @Test
    public void submitToMembers_callable_member_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        wait(exec(3).submitToMembers(callable(), asList(member(3))));
    }

    @Test
    public void submitToMembers_callable_member_callback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(callable(), asList(member(0)), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_callable_member_callback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(callable(), asList(member(3)), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
    }

    @Test
    public void submitToMembers_callable_selector_splitBrainProtection() throws Exception {
        wait(exec(0).submitToMembers(callable(), selector(0)));
    }

    @Test
    public void submitToMembers_callable_selector_noSplitBrainProtection() throws Exception {
        expectedException.expectCause(isA(SplitBrainProtectionException.class));
        wait(exec(3).submitToMembers(callable(), selector(3)));
    }

    @Test
    public void submitToMembers_callable_selector_callback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(callable(), selector(0), multiCallback);
        multiCallback.get();

    }

    @Test
    public void submitToMembers_callable_selector_callback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(callable(), selector(3), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
    }

    @Test
    public void submitToMembers_runnable_selector_callback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(runnable(), selector(0), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_runnable_selector_callback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(runnable(), selector(3), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
    }

    @Test
    public void submitToMembers_runnable_member_callback_splitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(runnable(), asList(member(0)), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_runnable_member_callback_noSplitBrainProtection() {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(runnable(), asList(member(3)), multiCallback);
        expectSplitBrainProtectionException(multiCallback);
    }

    @Test
    public void invokeAll_splitBrainProtection() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test
    public void invokeAll_noSplitBrainProtection() throws Exception {
        expectSplitBrainProtectionException(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test
    public void invokeAll_timeout_splitBrainProtection_short_timeout() throws Exception {
        List<? extends Future<?>> futures = exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS);

        // 10s is relatively short timeout -> there is some chance the task will be cancelled before it
        // had a chance to be executed. especially in slow environments -> we have to tolerate the CancellationException
        // see the test bellow for a scenario where the timeout is sufficiently long
        assertAllowedException(futures, CancellationException.class);
    }

    @Test
    public void invokeAll_timeout_splitBrainProtection_long_timeout() throws Exception {
        // 30s is a long enough timeout - the task should never be cancelled -> any exception means a test failure
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 30L, TimeUnit.SECONDS));
    }

    @Test
    public void invokeAll_timeout_noSplitBrainProtection() throws Exception {
        expectSplitBrainProtectionException(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10L, TimeUnit.SECONDS));
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

    @Test
    public void shutdown_noSplitBrainProtection() {
        try {
            exec(3, "shutdown").shutdown();
        } catch (SplitBrainProtectionException ex) {
            // best effort - server will throw with best effort basis, client will never throw due to API
        }
    }

    @Test
    public void shutdownNow_splitBrainProtection() {
        exec(0, "shutdownNow").shutdownNow();
    }

    @Test
    public void shutdownNow_noSplitBrainProtection() {
        try {
            exec(3, "shutdownNow").shutdownNow();
        } catch (SplitBrainProtectionException ex) {
            // best effort - server will throw with best effort basis, client will never throw due to API
        }
    }

    protected IExecutorService exec(int index) {
        return exec(index, splitBrainProtectionOn);
    }

    protected IExecutorService exec(int index, String postfix) {
        return exec(index, splitBrainProtectionOn, postfix);
    }

    private void wait(Map<Member, ? extends Future<?>> futures) throws Exception {
        for (Future f : futures.values()) {
            f.get();
        }
    }

    private Member member(int index) {
        return getNodeEngineImpl(cluster.getInstance(index)).getLocalMember();
    }

    private Object key(int index) {
        return generateKeyOwnedBy(cluster.getInstance(index), true);
    }

    private void expectSplitBrainProtectionException(MultiCallback callback) {
        try {
            callback.get();
        } catch (Exception ex) {
            if (!(ex instanceof SplitBrainProtectionException || ex.getCause() instanceof SplitBrainProtectionException)) {
                fail("Expected SplitBrainProtectionException but was " + ex);
            }
        }
    }

    private void expectSplitBrainProtectionException(Collection<? extends Future<?>> futures) {
        assertAllowedException(futures, SplitBrainProtectionException.class);
    }

    private void assertAllowedException(Collection<? extends Future<?>> futures, Class<?> allowedException) {
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                if (!(allowedException.isInstance(e) || allowedException.isInstance(e.getCause()))) {
                    fail("Expected " + allowedException + " but was " + e);
                }
            }
        }
    }

    private void expectSplitBrainProtectionException(Callback callback) {
        try {
            callback.get();
        } catch (Exception ex) {
            if (!(ex instanceof SplitBrainProtectionException || ex.getCause() instanceof SplitBrainProtectionException)) {
                fail("Expected SplitBrainProtectionException but was " + ex);
            }
        }
    }

    private void wait(Collection<? extends Future<?>> futures) throws ExecutionException, InterruptedException {
        for (Future<?> f : futures) {
            f.get();
        }
    }

    public static class ExecRunnable implements Runnable, Callable, Serializable {

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

    static class Selector implements MemberSelector {

        private int index;

        Selector(int index) {
            this.index = index;
        }

        @Override
        public boolean select(Member member) {
            return member.getAddress().getPort() % (getNode(cluster.getInstance(0)).getThisAddress().getPort() + index) == 0;
        }

        public static MemberSelector selector(int index) {
            return new Selector(index);
        }
    }

    static class Callback implements ExecutionCallback {
        static Semaphore finished;
        static Throwable throwable;

        Callback() {
            finished = new Semaphore(0);
            throwable = null;
        }

        @Override
        public void onResponse(Object response) {
            finished.release();
        }

        @Override
        public void onFailure(Throwable t) {
            finished.release();
            throwable = t;
        }

        public void get() {
            while (true) {
                try {
                    finished.tryAcquire(5, TimeUnit.SECONDS);
                    if (throwable != null) {
                        sneakyThrow(throwable);
                    }
                    return;
                } catch (InterruptedException ignored) {
                }
            }
        }

        public static Callback callback() {
            return new Callback();
        }
    }

    static class MultiCallback implements MultiExecutionCallback {

        Semaphore finished = new Semaphore(0);
        Throwable throwable;

        @Override
        public void onResponse(Member member, Object response) {
            if (response instanceof Throwable) {
                throwable = (Throwable) response;
            }
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            finished.release();
        }

        public void get() {
            while (true) {
                try {
                    if (!finished.tryAcquire(5, TimeUnit.SECONDS)) {
                        sneakyThrow(new TimeoutException());
                    }
                    if (throwable != null) {
                        sneakyThrow(throwable);
                    }
                    return;
                } catch (InterruptedException ignored) {
                }
            }
        }

        static MultiCallback multiCallback() {
            return new MultiCallback();
        }
    }

}
