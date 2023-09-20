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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.jet.sql.impl.connector.map.MapIndexScanP;
import com.hazelcast.test.OverridePropertyRule;
import org.junit.ClassRule;

import static com.hazelcast.test.OverridePropertyRule.set;

public class SqlIndexSmallFetchSizeTest extends SqlIndexTest {
    // Simulate index with many identical indexed values using existing test data
    // by redefining "many": "many" means > FETCH_SIZE_HINT.
    // This triggers special additional code paths when we have to fetch records
    // for single indexed value in batches.
    @ClassRule
    public static final OverridePropertyRule smallFetchSize = set(MapIndexScanP.FETCH_SIZE_HINT_PROPERTY_NAME, "3");
}
