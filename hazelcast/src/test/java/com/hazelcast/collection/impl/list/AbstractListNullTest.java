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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractListNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        assertThrowsNPE(l -> l.contains(null));
        assertThrowsNPE(l -> l.toArray((Object[]) null));
        assertThrowsNPE(l -> l.add(null));
        assertThrowsNPE(l -> l.remove(null));
        assertThrowsNPE(l -> l.containsAll(null));
        assertThrowsNPE(l -> l.addAll(null));
        assertThrowsNPE(l -> l.removeAll(null));
        assertThrowsNPE(l -> l.retainAll(null));
        assertThrowsNPE(l -> l.addItemListener(null, true));
        assertThrowsNPE(l -> l.removeItemListener(null));
    }

    private void assertThrowsNPE(ConsumerEx<IList<Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<IList<Object>> method) {
        try {
            method.accept(getDriver().getList(randomName()));
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
