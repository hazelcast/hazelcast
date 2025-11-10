/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql_nightly;

import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;

// This test is flaky because there is no guarantee on the order of received values
// when using multiple Kafka partitions.
@Category(NightlyTest.class)
public class SqlSTSInnerNonEquiJoinFaultToleranceStressTest extends SqlSTSInnerEquiJoinFaultToleranceStressTest {

    @Override
    protected String setupFetchingQuery() {
        expectedEventsCount = eventsToProcess - 1; // we expect fewer items for query below
        firstItemId = 2;                           // we expect first item to be [1, value-2]
        lastItemId = eventsToProcess;
        return "CREATE JOB " + JOB_NAME +
                " OPTIONS (" +
                " 'processingGuarantee'='" + processingGuarantee + "', 'snapshotIntervalMillis' = '750') " +
                " AS SINK INTO " + sinkTopic +
                " SELECT s1.__key, s2.this FROM s1 JOIN s2 ON s2.__key " +
                " BETWEEN s1.__key AND s1.__key + 1" +
                " WHERE s1.__key != s2.__key";
    }

    protected int getAllowedLag() {
        return 60;
    }
}
