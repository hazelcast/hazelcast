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

package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
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
        SqlDaySecondInterval restored = serializeAndCheck(original, SqlDataSerializerHook.INTERVAL_DAY_SECOND);

        checkEquals(original, restored, true);
    }
}
