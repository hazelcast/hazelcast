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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractHazelcastInstanceNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        assertThrowsNPE(c -> c.getQueue(null));
        assertThrowsNPE(c -> c.getTopic(null));
        assertThrowsNPE(c -> c.getSet(null));
        assertThrowsNPE(c -> c.getList(null));
        assertThrowsNPE(c -> c.getMap(null));
        assertThrowsNPE(c -> c.getReplicatedMap(null));
        assertThrowsNPE(c -> c.getMultiMap(null));
        assertThrowsNPE(c -> c.getRingbuffer(null));
        assertThrowsNPE(c -> c.getReliableTopic(null));
        assertThrowsNPE(c -> c.getExecutorService(null));
        assertThrowsNPE(c -> c.getDurableExecutorService(null));
        assertThrowsNPE(c -> c.newTransactionContext(null));
        assertThrowsNPE(c -> c.executeTransaction(null));
        assertThrowsNPE(c -> c.executeTransaction(null, context -> null));
        assertThrowsNPE(c -> c.executeTransaction(TransactionOptions.getDefault(), null));
        assertThrowsNPE(c -> c.getFlakeIdGenerator(null));
        assertThrowsNPE(c -> c.addDistributedObjectListener(null));
        assertThrowsNPE(c -> c.removeDistributedObjectListener(null));
        assertThrowsNPE(c -> c.getDistributedObject("", null));
        assertThrowsNPE(c -> c.getDistributedObject(null, ""));
        assertThrowsNPE(c -> c.getCardinalityEstimator(null));
        assertThrowsNPE(c -> c.getPNCounter(null));
        assertThrowsNPE(c -> c.getScheduledExecutorService(null));
    }

    private void assertThrowsNPE(ConsumerEx<HazelcastInstance> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<HazelcastInstance> method) {
        try {
            method.accept(getDriver());
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
