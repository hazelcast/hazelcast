/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;

public class UringFlowTest {

    private Uring uring;
    private SubmissionQueue sq;
    private CompletionQueue cq;

    @Before
    public void before() {
        uring = new Uring(16384, 0);

        sq = uring.submissionQueue();
        cq = uring.completionQueue();
    }

    @After
    public void after() {
        if (uring != null) {
            uring.close();
        }
    }

    @Test
    public void test() {
        int batchSize = 2000;
        long rounds = 50000;

        long total = batchSize * rounds;
        long completed = 0;
        long startMs = System.currentTimeMillis();
        for (long round = 0; round < rounds; round++) {
            if (round % 1000000 == 0) {
                System.out.println("at:" + round);
            }

            for (int k = 0; k < batchSize; k++) {
                sq.prepareNop(10);
            }
            int submittedEntries = sq.submit();
            assertEquals(batchSize, submittedEntries);
            int head = cq.acquireHead();

            for (; ; ) {
                int tail = cq.acquireTail();
                if (head == tail) {
                    // ringbuffer is empty
                    break;
                }

                int index = head & cq.ringMask();
                head++;
                completed++;
            }
            cq.releaseHead(head);
        }

        long duration = System.currentTimeMillis() - startMs;
        long thp = Math.round(total * 1000f / duration);
        DecimalFormat df = new DecimalFormat("#,###.00");
        System.out.println("thp:" + df.format(thp) + " ops");
        assertEquals(total, completed);
    }
}
