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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastSqlConformanceTest {
    @Test
    public void testConformance() {
        HazelcastSqlConformance conformance = HazelcastSqlConformance.INSTANCE;

        // ROW is not supported
        assertFalse(conformance.allowExplicitRowValueConstructor());

        // Allow "LIMIT x, y" in addition to "LIMIT x OFFSET y"
        assertTrue(conformance.isLimitStartCountAllowed());

        // Allow aliases for GROUP BY and HAVING, and ordinals for GROUP BY
        assertTrue(conformance.isGroupByAlias());
        assertTrue(conformance.isGroupByOrdinal());
        assertTrue(conformance.isHavingAlias());

        // FROM keyword is a must
        assertFalse(conformance.isFromRequired());

        // MINUS in addition to EXCEPT
        assertTrue(conformance.isMinusAllowed());

        // Allow A % B
        assertTrue(conformance.isPercentRemainderAllowed());

        // Allow FUNC in addition to FUNC()
        assertTrue(conformance.allowNiladicParentheses());

        // Allow <> in addition to !=
        assertTrue(conformance.isBangEqualAllowed());
    }
}
