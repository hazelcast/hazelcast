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

package com.hazelcast.internal.tpc.iouring;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SubmissionQueueTest {

    private IOUring uring;

    @After
    public void after() {
        if (uring != null) {
            uring.close();
        }
    }

    @Test
    public void test_construction() {
        int entries = 16384;
        uring = new IOUring(entries, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        assertSame(uring, sq.getIoUring());
        assertEquals(0, sq.localHead);
        assertEquals(0, sq.localTail);
        assertEquals(0, sq.acquireTail());
        assertEquals(0, sq.acquireHead());
        assertEquals(entries - 1, sq.ringMask);
        assertEquals(entries, sq.ringEntries);
    }

    @Test
    public void test_submitAndWait() {
        int entries = 16384;
        uring = new IOUring(entries, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        int count = 10;
        for (int k = 0; k < count; k++) {
            sq.offerNop(1);
        }

        int submitted = sq.submitAndWait();
        assertEquals(count, submitted);
    }

    // @Test
    public void test_submitAndWait_whenNoWork() {
        int entries = 16384;
        uring = new IOUring(entries, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        int submitted = sq.submitAndWait();
        assertEquals(0, submitted);
    }

    @Test
    public void test_submit_whenNoWork() {
        uring = new IOUring(16384, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        long localHead = sq.localHead;
        long head = sq.acquireHead();
        long localTail = sq.localTail;
        long tail = sq.tail();

        int submitted = sq.submit();

        assertEquals(0, submitted);
        assertEquals(localHead, sq.localHead);
        assertEquals(localTail, sq.localTail);
        assertEquals(head, sq.acquireHead());
        assertEquals(tail, sq.tail());
    }

    @Test
    public void test_submit_whenWork() {
        uring = new IOUring(16384, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        int pending = 10;
        for (int k = 0; k < pending; k++) {
            sq.offerNop(k);
        }

        long localHead = sq.localHead;
        long head = sq.acquireHead();
        long localTail = sq.localTail;
        long tail = sq.tail();

        int submitted = sq.submit();

        assertEquals(pending, submitted);
        assertEquals(localHead, sq.localHead);
        assertEquals(localTail, sq.localTail);
        //assertEquals(head, sq.acquireHead());
        assertEquals(pending, sq.tail());
    }
//
//    @Test
//    public void test_submit3_whenNoWork() {
//        uring = new IoUring(16384, 0);
//        SubmissionQueue sq = uring.getSubmissionQueue();
//
//        int submitted = sq.submit(0, 0, 0);
//        assertEquals(0, submitted);
//    }
//
//    @Test
//    public void test_submit3_whenWork() {
//        uring = new IoUring(16384, 0);
//        SubmissionQueue sq = uring.getSubmissionQueue();
//        sq.offerNop(1);
//        sq.offerNop(2);
//        sq.offerNop(3);
//
//        int submitted = sq.submit(3, 0, 0);
//        assertEquals(3, submitted);
//    }

    @Test
    public void test_offer() {
        int entries = 16;
        uring = new IOUring(entries, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();

        for (int k = 0; k < entries; k++) {
            sq.offerNop(1);
            assertEquals(0, sq.acquireTail());
            assertEquals(k + 1, sq.localTail);
        }
    }

    @Test
    public void test_offerWhenFull() {
        int entries = 16;
        uring = new IOUring(entries, 0);
        SubmissionQueue sq = uring.getSubmissionQueue();
        for (int k = 0; k < entries; k++) {
            assertTrue(sq.offerNop(1));
        }

        long tail = sq.tail();
        long localTail = sq.localTail;

        boolean success = sq.offerNop(1);

        assertFalse(success);
        assertEquals(tail, sq.tail());
        assertEquals(localTail, sq.localTail);
    }
}
