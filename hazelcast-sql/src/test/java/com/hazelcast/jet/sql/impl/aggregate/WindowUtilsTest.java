/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.Traverser;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WindowUtilsTest {

    @Test
    public void test_addWindowBounds() {
        // tumbling window
        check_addWindowBounds(9, 4, 4,
                8, 12);
        check_addWindowBounds(10, 4, 4,
                8, 12);
        check_addWindowBounds(11, 4, 4,
                8, 12);
        check_addWindowBounds(12, 4, 4,
                12, 16);

        // sliding window 4 by 2
        check_addWindowBounds(9, 4, 2,
                6, 10,
                8, 12);
        check_addWindowBounds(10, 4, 2,
                8, 12,
                10, 14);
        check_addWindowBounds(11, 4, 2,
                8, 12,
                10, 14);

        // sliding window 6 by 2
        check_addWindowBounds(11, 6, 2,
                6, 12,
                8, 14,
                10, 16);

        // Windows at the edge of the range - we overflow. Close to long range end
        // we overflow when calculating the ending window, then the currentStart is
        // greater than the negative window end, and we produce an empty traverser,
        // which actually is not that bad...
        // The behavior at negative extreme is also not that bad.
        check_addWindowBounds(Long.MAX_VALUE, 1, 1);
        check_addWindowBounds(Long.MIN_VALUE, 1, 1,
                Long.MIN_VALUE, Long.MIN_VALUE + 1);
        check_addWindowBounds(Long.MIN_VALUE, 2, 1);
        check_addWindowBounds(Long.MIN_VALUE, 2, 2,
                Long.MIN_VALUE, Long.MIN_VALUE + 2);

        // test with smaller integers - we simply overflow because we internally calculate with longs
        check_addWindowBounds((byte) 127, 4, 2,
                (byte) 124, (byte) -128,
                (byte) 126, (byte) -126);
        check_addWindowBounds((byte) -128, 4, 2,
                (byte) 126, (byte) -126,
                (byte) -128, (byte) -124);
    }

    /**
     * Calls WindowUtils.addWindowBounds with a record with the given
     * {@code timestamp} and window policy. Asserts that the returned
     * traverser contains the given {@code outputWindows}.
     */
    private void check_addWindowBounds(Object timestamp, long windowSize, long slideBy, Object ... outputWindows) {
        //noinspection SimplifiableAssertion
        assertTrue(outputWindows.length % 2 == 0);
        Traverser<JetSqlRow> traverser = WindowUtils.addWindowBounds(
                new JetSqlRow(TEST_SS, new Object[] {timestamp}), 0, slidingWinPolicy(windowSize, slideBy));
        for (int i = 0; i < outputWindows.length; i += 2) {
            Object[] expected = new Object[]{timestamp, outputWindows[i], outputWindows[i + 1]};
            assertArrayEquals(expected, traverser.next().getValues());
        }
        assertNull(traverser.next());
    }
}
