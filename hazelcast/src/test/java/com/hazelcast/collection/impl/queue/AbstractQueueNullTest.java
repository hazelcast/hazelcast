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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractQueueNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;

        assertThrowsNPE(q -> q.addItemListener(null, true));
        assertThrowsNPE(q -> q.removeItemListener(null));
        assertThrowsNPE(q -> q.add(null));
        assertThrowsNPE(q -> q.offer(null));
        assertThrowsNPE(q -> q.put(null));
        assertThrowsNPE(q -> q.offer(null, -1, sampleTimeUnit));
        assertThrowsNPE(q -> q.offer("a", -1, null));
        assertThrowsNPE(q -> q.poll(-1, null));
        assertThrowsNPE(q -> q.remove(null));
        assertThrowsNPE(q -> q.contains(null));
        assertThrowsNPE(q -> q.drainTo(null));
        assertThrowsNPE(q -> q.drainTo(null, 1));
        assertThrowsNPE(q -> q.toArray((Object[]) null));
        assertThrowsNPE(q -> q.containsAll(null));
        assertThrowsNPE(q -> q.addAll(null));
        assertThrowsNPE(q -> q.removeAll(null));
        assertThrowsNPE(q -> q.retainAll(null));
    }

    private void assertThrowsNPE(ConsumerEx<IQueue<Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<IQueue<Object>> method) {
        try {
            method.accept(getDriver().getQueue(randomName()));
            fail("Expected " + expectedExceptionClass
                    + " but there was no exception!");
        } catch (Exception e) {
            Assert.assertSame(expectedExceptionClass, e.getClass());
        }
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
