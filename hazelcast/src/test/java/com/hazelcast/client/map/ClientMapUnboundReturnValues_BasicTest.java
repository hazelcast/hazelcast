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

package com.hazelcast.client.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapUnboundReturnValues_BasicTest extends ClientMapUnboundReturnValuesTestSupport {

    @Test
    public void testClientMap_withException_NoPreCheck() {
        runClientMapTestWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientMap_withException_PreCheck() {
        runClientMapTestWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }

    @Test
    public void testClientMap_checkUnsupported() {
        runClientMapTestCheckUnsupported();
    }
}
