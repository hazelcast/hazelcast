/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Memoizers.memoizeConcurrent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@QuickTest
class ConcurrentMemoizingSupplierTest {

    @Test
    void when_memoizeConcurrent_then_threadSafe() {
        final Object obj = new Object();
        var rand = new Random();
        Supplier<Object> supplier = new Supplier<>() {
            volatile boolean supplied;

            @Override
            public Object get() {
                try {
                    Thread.sleep(rand.nextLong(10L, 50L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (supplied) {
                    throw new IllegalStateException("Supplier was already called once.");
                }
                supplied = true;
                return obj;
            }
        };

        // does not fail 100% with non-concurrent memoize, but about 50% of the time.
        List<Object> list = Stream.generate(memoizeConcurrent(supplier)).limit(4)
                                  .parallel()
                                  .toList();
        assertThat(list).allMatch(o -> o.equals(obj));
    }

    @Test
    public void when_memoizeConcurrentWithNullSupplier_then_exception() {
        Supplier<Object> supplier = () -> null;
        assertThatThrownBy(() -> memoizeConcurrent(supplier).get())
            .isInstanceOf(NullPointerException.class);
    }
}
