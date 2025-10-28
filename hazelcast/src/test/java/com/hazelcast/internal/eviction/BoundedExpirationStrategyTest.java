/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.eviction;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
@ParallelJVMTest
class BoundedExpirationStrategyTest {

    private final List<Integer> source = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9);

    @Test
    void testAllEntriesIteratedThrough() {
        List<Integer> expired = new ArrayList<>();

        new BoundedExpirationStrategy<>(source, expired::add, Integer.MAX_VALUE, Duration.of(1, ChronoUnit.DAYS)).doExpiration();

        assertThat(expired).isEqualTo(source);
    }

    @Test
    void testElementLimitReached() {
        List<Integer> expired = new ArrayList<>();

        var underTest = new BoundedExpirationStrategy<>(source, expired::add, 5, Duration.of(1, ChronoUnit.DAYS));
        underTest.doExpiration();
        assertThat(expired).isEqualTo(List.of(1, 2, 3, 4, 5));

        expired.clear();
        underTest.doExpiration();
        assertThat(expired).isEqualTo(List.of(6, 7, 8, 9));
    }

    @Test
    void testTimeBoundReached() {
        List<Integer> expired = new ArrayList<>();

        Duration timeLimit = Duration.ofSeconds(1);

        Iterable<Integer> elements = () -> new Iterator<>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < source.size();
            }

            @Override
            public Integer next() {
                if (index == 0) {
                    try {
                        Thread.sleep(2 * timeLimit.toMillis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return source.get(index++);
            }
        };

        var underTest = new BoundedExpirationStrategy<>(elements, expired::add, Integer.MAX_VALUE, timeLimit);
        underTest.doExpiration();
        assertThat(expired).isEqualTo(List.of(1));

        expired.clear();
        underTest.doExpiration();
        assertThat(expired).isEqualTo(List.of(2, 3, 4, 5, 6, 7, 8, 9));
    }
}
