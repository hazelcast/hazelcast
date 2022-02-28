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

package com.hazelcast.replicatedmap;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
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

public abstract class AbstractReplicatedMapNullTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    @Test
    public void testNullability() {
        EntryAdapter sampleEntryListener = new EntryAdapter();
        Predicate<Object, Object> samplePredicate = Predicates.alwaysTrue();
        TimeUnit sampleTimeUnit = TimeUnit.SECONDS;

        assertThrowsNPE(m -> m.putAll(null));
        assertThrowsNPE(m -> m.containsKey(null));
        assertThrowsNPE(m -> m.containsValue(null));
        assertThrowsNPE(m -> m.get(null));
        assertThrowsNPE(m -> m.put(null, ""));
        assertThrowsNPE(m -> m.put("", null));
        assertThrowsNPE(m -> m.remove(null));
        assertThrowsNPE(m -> m.remove(null, ""));

        assertThrowsNPE(m -> m.put(null, "", -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", null, -1, sampleTimeUnit));
        assertThrowsNPE(m -> m.put("", "", -1, null));

        assertThrowsNPE(m -> m.putIfAbsent(null, ""));
        assertThrowsNPE(m -> m.putIfAbsent("", null));

        assertThrowsNPE(m -> m.replace(null, "", ""));
        assertThrowsNPE(m -> m.replace(null, ""));

        assertThrowsNPE(m -> m.addEntryListener(null));
        assertThrowsNPE(m -> m.addEntryListener((EntryListener) null, ""));

        assertThrowsNPE(m -> m.addEntryListener((EntryListener) null, samplePredicate));
        assertThrowsNPE(m -> m.addEntryListener(sampleEntryListener, null));

        assertThrowsNPE(m -> m.addEntryListener((EntryListener) null, samplePredicate, ""));
        assertThrowsNPE(m -> m.addEntryListener(sampleEntryListener, null, ""));
        assertThrowsNPE(m -> m.removeEntryListener(null));
    }

    private void assertThrowsNPE(ConsumerEx<ReplicatedMap<Object, Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<ReplicatedMap<Object, Object>> method) {
        try {
            method.accept(getDriver().getReplicatedMap(MAP_NAME));
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
