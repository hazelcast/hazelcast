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
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SubmissionQueueTest {

    private Uring uring;

    @After
    public void after() {
        if (uring != null) {
            uring.close();
        }
    }

    @Test
    public void test_construction() {
        int entries = 16384;
        uring = new Uring(entries, 0);
        SubmissionQueue sq = uring.submissionQueue();

        assertSame(uring, sq.uring());
        Assert.assertEquals(0, sq.localHead);
        Assert.assertEquals(0, sq.localTail);
        Assert.assertEquals(0, sq.acquireTail());
        Assert.assertEquals(0, sq.acquireHead());
        Assert.assertEquals(entries - 1, sq.ringMask);
        Assert.assertEquals(entries, sq.ringEntries);
    }

    @Test
    public void test_submitAndWait() {
        int entries = 16384;
        uring = new Uring(entries, 0);
        SubmissionQueue sq = uring.submissionQueue();

        int count = 10;
        for (int k = 0; k < count; k++) {
            sq.offer_NOP(1);
        }

        int submitted = sq.submitAndWait();
        assertEquals(count, submitted);
    }

    // @Test
    public void test_submitAndWait_whenNoWork() {
        int entries = 16384;
        uring = new Uring(entries, 0);
        SubmissionQueue sq = uring.submissionQueue();

        int submitted = sq.submitAndWait();
        assertEquals(0, submitted);
    }

    @Test
    public void test_submit_whenNoWork() {
        uring = new Uring(16384, 0);
        SubmissionQueue sq = uring.submissionQueue();

        long localHead = sq.localHead;
        long head = sq.acquireHead();
        long localTail = sq.localTail;
        long tail = sq.tail();

        int submitted = sq.submit();

        assertEquals(0, submitted);
        Assert.assertEquals(localHead, sq.localHead);
        Assert.assertEquals(localTail, sq.localTail);
        Assert.assertEquals(head, sq.acquireHead());
        Assert.assertEquals(tail, sq.tail());
    }

    @Test
    public void test_submit_whenWork() {
        uring = new Uring(16384, 0);
        SubmissionQueue sq = uring.submissionQueue();

        int pending = 10;
        for (int k = 0; k < pending; k++) {
            sq.offer_NOP(k);
        }

        long localHead = sq.localHead;
        long head = sq.acquireHead();
        long localTail = sq.localTail;
        long tail = sq.tail();

        int submitted = sq.submit();

        assertEquals(pending, submitted);
        Assert.assertEquals(localHead, sq.localHead);
        Assert.assertEquals(localTail, sq.localTail);
        //assertEquals(head, sq.acquireHead());
        Assert.assertEquals(pending, sq.tail());
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
        uring = new Uring(entries, 0);
        SubmissionQueue sq = uring.submissionQueue();

        for (int k = 0; k < entries; k++) {
            sq.offer_NOP(1);
            Assert.assertEquals(0, sq.acquireTail());
            Assert.assertEquals(k + 1, sq.localTail);
        }
    }

    @Test
    public void test_offerWhenFull() {
        int entries = 16;
        uring = new Uring(entries, 0);
        SubmissionQueue sq = uring.submissionQueue();
        for (int k = 0; k < entries; k++) {
            assertTrue(sq.offer_NOP(1));
        }

        long tail = sq.tail();
        long localTail = sq.localTail;

        boolean success = sq.offer_NOP(1);

        assertFalse(success);
        Assert.assertEquals(tail, sq.tail());
        Assert.assertEquals(localTail, sq.localTail);
    }
}
