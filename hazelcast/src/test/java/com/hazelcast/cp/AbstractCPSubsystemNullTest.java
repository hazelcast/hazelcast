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

package com.hazelcast.cp;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractCPSubsystemNullTest extends HazelcastTestSupport {

    @Test
    public void testNullability() {
        assertThrowsNPE(cp -> cp.getAtomicLong(null));
        assertThrowsNPE(cp -> cp.getAtomicReference(null));
        assertThrowsNPE(cp -> cp.getCountDownLatch(null));
        assertThrowsNPE(cp -> cp.getLock(null));
        assertThrowsNPE(cp -> cp.getSemaphore(null));
    }

    private void assertThrowsNPE(ConsumerEx<CPSubsystem> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<CPSubsystem> method) {
        try {
            method.accept(getDriver().getCPSubsystem());
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
                System.out.println(e.getMessage());
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected abstract HazelcastInstance getDriver();
}
