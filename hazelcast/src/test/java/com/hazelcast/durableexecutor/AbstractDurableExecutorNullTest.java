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

package com.hazelcast.durableexecutor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public abstract class AbstractDurableExecutorNullTest extends HazelcastTestSupport {

    public static final String RANDOM_NAME = randomName();

    @Test
    public void testNullability() {
        Runnable sampleRunnable = (Runnable & Serializable) () -> {
        };
        Callable<Object> sampleCallable = (Callable & Serializable) () -> "";

        assertThrowsNPE(s -> s.submit((Callable) null));
        assertThrowsNPE(s -> s.submit((Runnable) null, ""));
        getDriver().getDurableExecutorService(RANDOM_NAME)
                   .submit(sampleRunnable, null);
        assertThrowsNPE(s -> s.submit((Runnable) null));
        assertThrowsNPE(s -> s.executeOnKeyOwner((Runnable) null, ""));
        assertThrowsNPE(s -> s.executeOnKeyOwner(sampleRunnable, null));
        assertThrowsNPE(s -> s.submitToKeyOwner((Callable) null, ""));
        getDriver().getDurableExecutorService(RANDOM_NAME)
                   .submitToKeyOwner(sampleCallable, "");
        assertThrowsNPE(s -> s.submitToKeyOwner((Runnable) null, ""));
        assertThrowsNPE(s -> s.submitToKeyOwner(sampleRunnable, null));
    }

    private void assertThrowsNPE(ConsumerEx<DurableExecutorService> method) {
        DurableExecutorService executorService = getDriver().getDurableExecutorService(RANDOM_NAME);
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
