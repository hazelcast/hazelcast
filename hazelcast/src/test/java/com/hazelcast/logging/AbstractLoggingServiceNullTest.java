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

package com.hazelcast.logging;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;
import java.util.logging.Level;

import static org.junit.Assert.fail;

public abstract class AbstractLoggingServiceNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        LogListener dummyLogListener = logEvent -> {

        };
        if (isNotClient()) {
            assertThrowsNPE(s -> s.addLogListener(null, dummyLogListener));
            assertThrowsNPE(s -> s.addLogListener(Level.FINEST, null));
            assertThrowsNPE(s -> s.removeLogListener(null));
        }
        assertThrowsNPE(s -> s.getLogger((String) null));
        assertThrowsNPE(s -> s.getLogger((Class) null));
    }

    private void assertThrowsNPE(ConsumerEx<LoggingService> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<LoggingService> method) {
        try {
            method.accept(getDriver().getLoggingService());
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
