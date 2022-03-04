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

package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapUnboundedReturnValues_StringKeyTest extends MapUnboundedReturnValuesTestSupport {

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMap_SmallLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMap_MediumLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, MEDIUM_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMap_LargeLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, LARGE_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }

}
