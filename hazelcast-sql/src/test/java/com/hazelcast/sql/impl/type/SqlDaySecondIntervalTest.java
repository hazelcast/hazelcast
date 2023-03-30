/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.type;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlDaySecondIntervalTest extends CoreSqlTestSupport {
    @Test
    public void testEquals() {
        SqlDaySecondInterval value = new SqlDaySecondInterval(1);

        checkEquals(value, new SqlDaySecondInterval(1), true);
        checkEquals(value, new SqlDaySecondInterval(2), false);
    }

    @Test
    public void testSerialization() {
        SqlDaySecondInterval original = new SqlDaySecondInterval(1);
        SqlDaySecondInterval restored = serializeAndCheck(original, JetSqlSerializerHook.INTERVAL_DAY_SECOND);

        checkEquals(original, restored, true);
    }
}
