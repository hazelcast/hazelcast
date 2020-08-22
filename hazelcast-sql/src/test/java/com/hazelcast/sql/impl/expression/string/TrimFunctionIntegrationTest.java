/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_input() {
        put("abcde");
        checkValueInternal("SELECT TRIM(LEADING 'abc' FROM this) FROM map", SqlColumnType.VARCHAR, "de");

        // TODO
//        checkValueInternal("SELECT TRIM(TRAILING 'abc' FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(BOTH 'abc' FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT TRIM(LEADING FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(TRAILING FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(BOTH FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
    }
}
