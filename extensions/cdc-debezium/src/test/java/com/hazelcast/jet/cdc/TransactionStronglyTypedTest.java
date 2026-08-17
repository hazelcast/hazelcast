/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuickTest
@RunWith(HazelcastParallelClassRunner.class)
class TransactionStronglyTypedTest {

    @Test
    void comparesCorrectly() throws InterruptedException {
        var t1 = new TransactionStronglyTyped(101, "2025-07-24 18:32:50.001",
                                              "2025-07-24T18:32:50.001Z",
                                              "2025-09-24", "PT2H30M",
                                              Instant.now().toString());
        Thread.sleep(100);
        var t2 = new TransactionStronglyTyped(101, "2025-07-24 18:32:50.002",
                                              "2025-07-24T18:32:50.002Z",
                                              "2025-09-24", "PT2H30M",
                                              Instant.now().toString());
        assertEquals(t1, t2);
    }
}
