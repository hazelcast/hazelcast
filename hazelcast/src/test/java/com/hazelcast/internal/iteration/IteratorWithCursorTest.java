/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.iteration;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IteratorWithCursorTest extends HazelcastTestSupport {

    private IteratorWithCursor<Integer> iterator;
    private static final int SIZE = 1000;
    private final UUID initialCursorId = UuidUtil.newUnsecureUUID();

    @Before
    public void setUp() {
        List<Integer> items = new ArrayList<>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            items.add(i);
        }
        iterator = new IteratorWithCursor<>(items.iterator(), initialCursorId);
    }

    @Test
    public void testIterateWithInitialCursorId() {
        int pageSize = 100;
        IterationResult<Integer> result = iterator.iterate(initialCursorId, pageSize);
        // the first iteration does not forget anything
        assertIterationResult(result, initialCursorId, null, 0, pageSize);
    }

    @Test
    public void testIterate_ContinuesWhereLeftOff() {
        int pageSize = 100;
        UUID cursorId = iterator.iterate(initialCursorId, pageSize).getCursorId();

        assertIterationResult(iterator.iterate(cursorId, 100), cursorId, initialCursorId, 100, pageSize);
    }

    @Test
    public void testIterateOnPreviousCursor_ReturnsTheSameResult_WithNoStateMutation() {
        int pageSize = 100;
        IterationResult<Integer> result = iterator.iterate(initialCursorId, pageSize);
        assertIterationResult(result, initialCursorId, null, 0, pageSize);
        assertIterationResult(iterator.iterate(initialCursorId, 100), initialCursorId, null, 0, pageSize);
    }

    @Test
    public void testIterateOnCurrentCursor_ProgressIterator() {
        int pageSize = 100;
        IterationResult<Integer> result = iterator.iterate(initialCursorId, pageSize);
        assertIterationResult(result, initialCursorId, null, 0, pageSize);
        UUID cursorId = result.getCursorId();
        assertIterationResult(iterator.iterate(cursorId, 100), cursorId, initialCursorId, 100, pageSize);
    }

    @Test
    public void testConstructorSetsLastAccessTime() {
        long lastAccessTime = iterator.getLastAccessTime();
        assertThat(lastAccessTime).isGreaterThan(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        assertThat(lastAccessTime).isLessThanOrEqualTo(System.currentTimeMillis());
    }

    @Test
    public void testIterateOnCurrentCursorUpdatesLastAccessTime() throws InterruptedException {
        long lastAccessTime = iterator.getLastAccessTime();
        int pageSize = 100;
        Thread.sleep(100);
        iterator.iterate(initialCursorId, pageSize);
        assertThat(iterator.getLastAccessTime()).isGreaterThan(lastAccessTime);
    }

    @Test
    public void testIterateOnPreviousCursorUpdatesLastAccessTime() throws InterruptedException {
        int pageSize = 100;
        UUID cursorId = iterator.iterate(initialCursorId, pageSize).getCursorId();
        long lastAccessTime = iterator.getLastAccessTime();
        Thread.sleep(100);
        iterator.iterate(cursorId, pageSize);
        assertThat(iterator.getLastAccessTime()).isGreaterThan(lastAccessTime);
    }

    @Test
    public void testCursorIdOtherThan_PreviousOrCurrent_Throws() {
        UUID cursorId = iterator.iterate(initialCursorId, 100).getCursorId();
        iterator.iterate(cursorId, 100);
        // prevCursorId == cursorId, cursorId == new id
        assertThatThrownBy(() -> iterator.iterate(initialCursorId, 100)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The cursor id " + initialCursorId
                        + " is not the current cursor id nor the previous cursor id.");
    }


    private void assertIterationResult(IterationResult<Integer> result, UUID prevCursorId, UUID cursorIdToForget, int start,
                                       int size) {
        assertThat(result.getCursorIdToForget()).isEqualTo(cursorIdToForget);
        assertThat(result.getCursorId()).isNotEqualTo(prevCursorId);
        List<Integer> page = result.getPage();
        assertThat(page).hasSize(size);
        for (int i = 0; i < page.size(); i++) {
            assertThat(page.get(i)).isEqualTo(i + start);
        }
    }
}
