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

package com.hazelcast.executor;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public abstract class AbstractExecutorNullTest extends HazelcastTestSupport {

    public static final String RANDOM_NAME = randomName();

    @Test
    public void testNullability() {
        Runnable sampleRunnable = (Runnable & Serializable) () -> {
        };
        Callable<Object> sampleCallable = (Callable & Serializable) () -> "";
        Member sampleMember = getDriver().getCluster().getMembers().iterator().next();
        Set<Member> sampleMembers = Collections.singleton(sampleMember);
        MemberSelector sampleSelector = member -> true;
        ExecutionCallback<Object> sampleCallback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {

            }
        };
        MultiExecutionCallback sampleMultiExecutionCallback = new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {

            }

            @Override
            public void onComplete(Map<Member, Object> values) {

            }
        };

        assertThrowsNPE(s -> s.execute((Runnable) null, sampleSelector));
        assertThrowsNPE(s -> s.execute(sampleRunnable, null));
        assertThrowsNPE(s -> s.executeOnKeyOwner(null, ""));
        assertThrowsNPE(s -> s.executeOnKeyOwner(sampleRunnable, null));
        assertThrowsNPE(s -> s.executeOnMember(null, sampleMember));
        assertThrowsNPE(s -> s.executeOnMember(sampleRunnable, null));
        assertThrowsNPE(s -> s.executeOnMembers(null, sampleMembers));
        assertThrowsNPE(s -> s.executeOnMembers(sampleRunnable, (Collection<Member>) null));
        assertThrowsNPE(s -> s.executeOnMembers(null, sampleSelector));
        assertThrowsNPE(s -> s.executeOnMembers(sampleRunnable, (MemberSelector) null));
        assertThrowsNPE(s -> s.executeOnAllMembers(null));

        System.out.println("------------1");

        assertThrowsNPE(s -> s.submit((Callable) null, sampleSelector));
        assertThrowsNPE(s -> s.submit(sampleCallable, (MemberSelector) null));
        assertThrowsNPE(s -> s.submitToKeyOwner((Callable) null, ""));
        assertThrowsNPE(s -> s.submitToKeyOwner(sampleCallable, null));
        assertThrowsNPE(s -> s.submitToMember((Callable) null, sampleMember));
        assertThrowsNPE(s -> s.submitToMember(sampleCallable, null));
        assertThrowsNPE(s -> s.submitToMembers((Callable) null, sampleMembers));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, (Collection<Member>) null));
        assertThrowsNPE(s -> s.submitToMembers((Callable) null, sampleSelector));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, (MemberSelector) null));
        assertThrowsNPE(s -> s.submitToAllMembers(null));

        System.out.println("------------2");

        assertThrowsNPE(s -> s.submit((Runnable) null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submit(sampleRunnable, (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submit((Runnable) null, sampleSelector, sampleCallback));
        assertThrowsNPE(s -> s.submit(sampleRunnable, (MemberSelector) null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submit(sampleRunnable, sampleSelector, (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submitToKeyOwner((Runnable) null, "", sampleCallback));
        assertThrowsNPE(s -> s.submitToKeyOwner(sampleRunnable, null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submitToKeyOwner(sampleRunnable, "", (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submitToMember((Runnable) null, sampleMember, sampleCallback));
        assertThrowsNPE(s -> s.submitToMember(sampleRunnable, null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submitToMember(sampleRunnable, sampleMember, (ExecutionCallback) null);
        assertThrowsNPE(s -> s.submitToMembers((Runnable) null, sampleMembers, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleRunnable, (Collection) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleRunnable, sampleMembers, (MultiExecutionCallback) null));

        assertThrowsNPE(s -> s.submitToMembers((Runnable) null, sampleSelector, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleRunnable, (MemberSelector) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleRunnable, sampleSelector, (MultiExecutionCallback) null));

        assertThrowsNPE(s -> s.submitToAllMembers((Runnable) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToAllMembers(sampleRunnable, null));

        System.out.println("------------3");

        assertThrowsNPE(s -> s.submit((Callable<Object>) null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submit(sampleCallable, (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submit((Callable) null, sampleSelector, sampleCallback));
        assertThrowsNPE(s -> s.submit(sampleCallable, (MemberSelector) null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submit(sampleCallable, sampleSelector, (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submitToKeyOwner((Callable) null, "", sampleCallback));
        assertThrowsNPE(s -> s.submitToKeyOwner(sampleCallable, null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submitToKeyOwner(sampleCallable, "", (ExecutionCallback) null);

        assertThrowsNPE(s -> s.submitToMember((Callable) null, sampleMember, sampleCallback));
        assertThrowsNPE(s -> s.submitToMember(sampleCallable, null, sampleCallback));
        getDriver().getExecutorService(RANDOM_NAME)
                   .submitToMember(sampleCallable, sampleMember, (ExecutionCallback) null);
        assertThrowsNPE(s -> s.submitToMembers((Callable) null, sampleMembers, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, (Collection) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, sampleMembers, (MultiExecutionCallback) null));

        assertThrowsNPE(s -> s.submitToMembers((Callable) null, sampleSelector, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, (MemberSelector) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToMembers(sampleCallable, sampleSelector, (MultiExecutionCallback) null));

        assertThrowsNPE(s -> s.submitToAllMembers((Callable) null, sampleMultiExecutionCallback));
        assertThrowsNPE(s -> s.submitToAllMembers(sampleCallable, null));
    }

    private void assertThrowsNPE(ConsumerEx<IExecutorService> method) {
        IExecutorService executorService = getDriver().getExecutorService(RANDOM_NAME);
        assertThrows(NullPointerException.class, () -> method.accept(executorService));
    }

    @FunctionalInterface
    public interface ConsumerEx<T> extends Consumer<T> {
        void acceptEx(T t) throws Exception;

        @Override
        default void accept(T t) {
            try {
                acceptEx(t);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected abstract HazelcastInstance getDriver();
}
