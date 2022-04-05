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

package com.hazelcast.multimap;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractMultiMapNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        EntryAdapter sampleEntryListener = new EntryAdapter();
        Predicate<Object, Object> samplePredicate = Predicates.alwaysTrue();
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;

        assertThrowsNPE(m -> m.put(null, ""));
        assertThrowsNPE(m -> m.put("", null));
        assertThrowsNPE(m -> m.get(null));
        assertThrowsNPE(m -> m.remove(null));
        assertThrowsNPE(m -> m.remove(null, ""));
        assertThrowsNPE(m -> m.remove("", null));
        assertThrowsNPE(m -> m.delete(null));
        assertThrowsNPE(m -> m.containsKey(null));
        assertThrowsNPE(m -> m.containsValue(null));
        assertThrowsNPE(m -> m.containsEntry(null, ""));
        assertThrowsNPE(m -> m.containsEntry("", null));
        assertThrowsNPE(m -> m.valueCount(null));

        if (isNotClient()) {
            assertThrowsNPE(m -> m.addLocalEntryListener(null));
        }
        assertThrowsNPE(m -> m.addEntryListener(null, "", true));

        assertThrowsNPE(m -> m.addEntryListener(sampleEntryListener, null, true));
        assertThrowsNPE(m -> m.addEntryListener(null, samplePredicate, true));
        assertThrowsNPE(m -> m.addEntryListener(sampleEntryListener, null, true));

        assertThrowsNPE(m -> m.removeEntryListener(null));

        assertThrowsNPE(m -> m.lock(null));
        assertThrowsNPE(m -> m.lock(null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.lock("", -1, null));
        assertThrowsNPE(m -> m.isLocked(null));


        assertThrowsNPE(m -> m.tryLock(null));
        assertThrowsNPE(m -> m.tryLock(null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.tryLock(null, -1, sampleTimeUnit, -1, sampleTimeUnit));

        assertThrowsNPE(m -> m.unlock(null));
        assertThrowsNPE(m -> m.forceUnlock(null));
    }

    private void assertThrowsNPE(ConsumerEx<MultiMap<Object, Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<MultiMap<Object, Object>> method) {
        try {
            method.accept(getDriver().getMultiMap(randomName()));
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

    protected abstract boolean isNotClient();

    protected abstract HazelcastInstance getDriver();
}
