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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class AbstractScheduledExecutorNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;
        Runnable sampleRunnable = () -> {
        };
        Callable<String> sampleCallable = () -> "";
        Member sampleMember = new MemberImpl();
        Set<Member> sampleMembers = Collections.singleton(sampleMember);

        assertThrowsNPE(s -> s.schedule((Runnable) null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.schedule(sampleRunnable, 1, null));
        assertThrowsNPE(s -> s.schedule((Callable<String>) null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.schedule(sampleCallable, 1, null));
        assertThrowsNPE(s -> s.scheduleAtFixedRate((Runnable) null, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleAtFixedRate(sampleRunnable, 1, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMember((Runnable) null, sampleMember, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMember(sampleRunnable, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMember(sampleRunnable, sampleMember, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMember((Callable<String>) null, sampleMember, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMember((Callable<String>) null, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMember(sampleCallable, sampleMember, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMemberAtFixedRate((Runnable) null, sampleMember, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMemberAtFixedRate(sampleRunnable, null, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMemberAtFixedRate(sampleRunnable, sampleMember, 1, 1, null));

        assertThrowsNPE(s -> s.scheduleOnKeyOwner((Runnable) null, "", 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwner(sampleRunnable, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwner(sampleRunnable, "", 1, null));

        assertThrowsNPE(s -> s.scheduleOnKeyOwner((Callable<String>) null, "", 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwner(sampleCallable, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwner(sampleCallable, "", 1, null));


        assertThrowsNPE(s -> s.scheduleOnKeyOwnerAtFixedRate((Runnable) null, "", 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwnerAtFixedRate(sampleRunnable, null, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnKeyOwnerAtFixedRate(sampleRunnable, "", 1, 1, null));

        assertThrowsNPE(s -> s.scheduleOnAllMembers((Runnable) null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnAllMembers(sampleRunnable, 1, null));

        assertThrowsNPE(s -> s.scheduleOnAllMembers((Callable<String>) null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnAllMembers(sampleCallable, 1, null));

        assertThrowsNPE(s -> s.scheduleOnAllMembersAtFixedRate((Runnable) null, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnAllMembersAtFixedRate(sampleRunnable, 1, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMembers((Runnable) null, sampleMembers, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembers(sampleRunnable, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembers(sampleRunnable, sampleMembers, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMembers((Callable<String>) null, sampleMembers, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembers((Callable<String>) null, null, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembers(sampleCallable, sampleMembers, 1, null));

        assertThrowsNPE(s -> s.scheduleOnMembersAtFixedRate((Runnable) null, sampleMembers, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembersAtFixedRate(sampleRunnable, null, 1, 1, sampleTimeUnit));
        assertThrowsNPE(s -> s.scheduleOnMembersAtFixedRate(sampleRunnable, sampleMembers, 1, 1, null));

        assertThrowsNPE(s -> s.getScheduledFuture(null));
    }

    private void assertThrowsNPE(ConsumerEx<IScheduledExecutorService> method) {
        IScheduledExecutorService executorService = getDriver().getScheduledExecutorService(randomName());
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
