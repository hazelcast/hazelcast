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

package com.hazelcast.quorum.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.Callback.callback;
import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.ExecRunnable.callable;
import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.ExecRunnable.runnable;
import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.MultiCallback.multiCallback;
import static com.hazelcast.quorum.executor.ExecutorQuorumWriteTest.Selector.selector;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyOwnedBy;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ExecutorQuorumWriteTest extends AbstractExecutorQuorumTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
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
    public void executeOnAllMembers_quorum() throws Exception {
        exec(0).executeOnAllMembers(runnable());
    }

    @Test
    public void executeOnAllMembers_noQuorum() throws Exception {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnAllMembers(runnable());
    }

    @Test
    public void executeOnKeyOwner_quorum() throws Exception {
        exec(0).executeOnKeyOwner(runnable(), key(0));
    }

    @Test
    public void executeOnKeyOwner_noQuorum() throws Exception {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnKeyOwner(runnable(), key(3));
    }

    @Test
    public void executeOnMember_quorum() throws Exception {
        exec(0).executeOnMember(runnable(), member(0));
    }

    @Test
    public void executeOnMember_noQuorum() throws Exception {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnMember(runnable(), member(3));
    }

    @Test
    public void executeOnMembers_collection_quorum() throws Exception {
        exec(0).executeOnMembers(runnable(), asList(member(0)));
    }

    @Test
    public void executeOnMembers_collection_noQuorum() throws Exception {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnMembers(runnable(), asList(member(3)));
    }

    @Test
    public void executeOnMembers_selector_quorum() throws Exception {
        exec(0).executeOnMembers(runnable(), selector(0));
    }

    @Test
    public void executeOnMembers_selector_noQuorum() throws Exception {
        // fire and forget operation, no quorum exception propagation
        // expectedException.expectCause(isA(QuorumException.class));
        exec(3).executeOnMembers(runnable(), selector(3));
    }

    @Test
    public void execute_quorum() throws Exception {
        exec(0).execute(runnable());
    }

    @Test
    public void execute_noQuorum() throws Exception {
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
    public void submit_runnable_selector_quorum() throws Exception {
        exec(0).submit(runnable(), selector(0)).get();
    }

    @Test
    public void submit_runnable_selector_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submit(runnable(), selector(3)).get();
    }

    @Test
    public void submit_runnable_selector_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submit(runnable(), selector(0), callback);
        callback.get();
    }

    @Test
    public void submit_runnable_selector_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submit(runnable(), selector(3), callback());
        expectQuorumException(callback);
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
    public void submit_callable_selector_quorum() throws Exception {
        exec(0).submit(callable(), selector(0)).get();
    }

    @Test
    public void submit_callable_selector_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submit(callable(), selector(3)).get();
    }

    @Test
    public void submit_callable_selector_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submit(callable(), selector(0), callback);
        callback.get();
    }

    @Test
    public void submit_callable_selector_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submit(callable(), selector(3), callback());
        expectQuorumException(callback);
    }

    @Test
    public void submitToAllMembers_callable_quorum() throws Exception {
        wait(exec(0).submitToAllMembers(callable()));
    }

    @Test
    public void submitToAllMembers_callable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        wait(exec(3).submitToAllMembers(callable()));
    }

    @Test
    public void submitToAllMembers_callable_multiCallback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToAllMembers(callable(), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToAllMembers_callable_multiCallback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToAllMembers(callable(), multiCallback);
        expectQuorumException(multiCallback);
    }

    @Test
    public void submitToAllMembers_runnable_multiCallback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToAllMembers(runnable(), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToAllMembers_runnable_multiCallback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToAllMembers(runnable(), multiCallback);
        expectQuorumException(multiCallback);
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
    public void submitToKeyOwner_runnable_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submitToKeyOwner(runnable(), key(0), callback);
        callback.get();
    }

    @Test
    public void submitToKeyOwner_runnable_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submitToKeyOwner(runnable(), key(3), callback);
        expectQuorumException(callback);
    }

    @Test
    public void submitToKeyOwner_callable_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submitToKeyOwner(callable(), key(0), callback);
        callback.get();
    }

    @Test
    public void submitToKeyOwner_callable_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submitToKeyOwner(callable(), key(3), callback);
        expectQuorumException(callback);
    }

    @Test
    public void submitToMember_callable_quorum() throws Exception {
        exec(0).submitToMember(callable(), member(0)).get();
    }

    @Test
    public void submitToMember_callable_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        exec(3).submitToMember(callable(), member(3)).get();
    }

    @Test
    public void submitToMember_runnable_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submitToMember(runnable(), member(0), callback);
        callback.get();
    }

    @Test
    public void submitToMember_runnable_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submitToMember(runnable(), member(3), callback);
        expectQuorumException(callback);
    }

    @Test
    public void submitToMember_callable_callback_quorum() throws Exception {
        Callback callback = callback();
        exec(0).submitToMember(callable(), member(0), callback);
        callback.get();
    }

    @Test
    public void submitToMember_callable_callback_noQuorum() throws Exception {
        Callback callback = callback();
        exec(3).submitToMember(callable(), member(3), callback);
        expectQuorumException(callback);
    }

    @Test
    public void submitToMembers_callable_member_quorum() throws Exception {
        wait(exec(0).submitToMembers(callable(), asList(member(0))));
    }

    @Test
    public void submitToMembers_callable_member_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        wait(exec(3).submitToMembers(callable(), asList(member(3))));
    }

    @Test
    public void submitToMembers_callable_member_callback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(callable(), asList(member(0)), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_callable_member_callback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(callable(), asList(member(3)), multiCallback);
        expectQuorumException(multiCallback);
    }

    @Test
    public void submitToMembers_callable_selector_quorum() throws Exception {
        wait(exec(0).submitToMembers(callable(), selector(0)));
    }

    @Test
    public void submitToMembers_callable_selector_noQuorum() throws Exception {
        expectedException.expectCause(isA(QuorumException.class));
        wait(exec(3).submitToMembers(callable(), selector(3)));
    }

    @Test
    public void submitToMembers_callable_selector_callback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(callable(), selector(0), multiCallback);
        multiCallback.get();

    }

    @Test
    public void submitToMembers_callable_selector_callback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(callable(), selector(3), multiCallback);
        expectQuorumException(multiCallback);
    }

    @Test
    public void submitToMembers_runnable_selector_callback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(runnable(), selector(0), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_runnable_selector_callback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(runnable(), selector(3), multiCallback);
        expectQuorumException(multiCallback);
    }

    @Test
    public void submitToMembers_runnable_member_callback_quorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(0).submitToMembers(runnable(), asList(member(0)), multiCallback);
        multiCallback.get();
    }

    @Test
    public void submitToMembers_runnable_member_callback_noQuorum() throws Exception {
        MultiCallback multiCallback = multiCallback();
        exec(3).submitToMembers(runnable(), asList(member(3)), multiCallback);
        expectQuorumException(multiCallback);
    }

    @Test
    public void invokeAll_quorum() throws Exception {
        wait(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test
    public void invokeAll_noQuorum() throws Exception {
        expectQuorumException(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable())));
    }

    @Test
    public void invokeAll_timeout_quorum() throws Exception {
        try {
            expectQuorumException(exec(0).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10l, TimeUnit.SECONDS));
        } catch (UnsupportedOperationException ex) {
        }
    }

    @Test
    public void invokeAll_timeout_noQuorum() throws Exception {
        try {
            expectQuorumException(exec(3).invokeAll(Arrays.<Callable<Object>>asList(callable(), callable()), 10l, TimeUnit.SECONDS));
        } catch (UnsupportedOperationException ex) {
        }
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
        exec(0).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10l, TimeUnit.SECONDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void invokeAny_timeout_noQuorum() throws Exception {
        exec(3).invokeAny(Arrays.<Callable<Object>>asList(callable(), callable()), 10l, TimeUnit.SECONDS);
    }

    @Test
    public void shutdown_quorum() throws Exception {
        exec(0, "shutdown").shutdown();
    }

    @Test
    public void shutdown_noQuorum() throws Exception {
        try {
            exec(3, "shutdown").shutdown();
        } catch(QuorumException ex) {
            // best effort - server will throw with best effort basis, client will never throw due to API
        }
    }

    @Test
    public void shutdownNow_quorum() throws Exception {
        exec(0, "shutdownNow").shutdownNow();
    }

    @Test
    public void shutdownNow_noQuorum() throws Exception {
        try {
            exec(3, "shutdownNow").shutdownNow();
        } catch(QuorumException ex) {
            // best effort - server will throw with best effort basis, client will never throw due to API
        }
    }

    protected IExecutorService exec(int index) {
        return exec(index, quorumType);
    }

    protected IExecutorService exec(int index, String postfix) {
        return exec(index, quorumType, postfix);
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

    static class Selector implements MemberSelector {

        private int index;

        public Selector(int index) {
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

        public Callback() {
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
                } catch (InterruptedException e) {
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
                    if (finished.tryAcquire(5, TimeUnit.SECONDS) == false) {
                        sneakyThrow(new TimeoutException());
                    }
                    if (throwable != null) {
                        sneakyThrow(throwable);
                    }
                    return;
                } catch (InterruptedException e) {
                }
            }
        }

        public static MultiCallback multiCallback() {
            return new MultiCallback();
        }
    }

    private void wait(Map<Member, Future<?>> futures) throws Exception {
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

    private void expectQuorumException(MultiCallback callback) {
        try {
            callback.get();
        } catch (Exception ex) {
            if (!(ex instanceof QuorumException || ex.getCause() instanceof QuorumException))
                fail("Expected QuorumException but was " + ex);
        }
    }

    private void expectQuorumException(Collection<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception ex) {
                if (!(ex instanceof QuorumException || ex.getCause() instanceof QuorumException))
                    fail("Expected QuorumException but was " + ex);
            }
        }
    }

    private void expectQuorumException(Callback callback) {
        try {
            callback.get();
        } catch (Exception ex) {
            if (!(ex instanceof QuorumException || ex.getCause() instanceof QuorumException))
                fail("Expected QuorumException but was " + ex);
        }
    }

    private void wait(Collection<Future<?>> futures) throws ExecutionException, InterruptedException {
        for (Future f : futures) {
            f.get();
        }
    }
}
