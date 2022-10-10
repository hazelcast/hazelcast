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

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_ENTER_GETEVENTS;

// https://unixism.net/loti/low_level.html
// Old toy; can be thrown
public class LowLevelMain {

    public static void main(String[] args) throws InterruptedException {
        IOUring uring = new IOUring(16384, 0);

        SubmissionQueue sq = uring.getSubmissionQueue();

        scheduleNop(sq);
        scheduleNop(sq);
        scheduleNop(sq);
        scheduleNop(sq);
        int submittedEntries = sq.enter(4, 1, IORING_ENTER_GETEVENTS);

        System.out.println("submittedEntries:" + submittedEntries);
        CompletionQueue cq = uring.getCompletionQueue();
        int head = cq.acquireHead();// no acquire needed

        for (; ; ) {
            int tail = cq.acquireTail();
            if (head == tail) {
                break;
            }

            int index = head & cq.ringMask();
            System.out.println("index:" + index);
            head++;
        }
    }

    private static void scheduleNop(SubmissionQueue sq) {
        int tail = sq.acquireTail();
        int next_tail = tail;
        next_tail++;

        int index = tail & sq.ringMask;
        System.out.println("index:" + index);

        sq.offerNop(10);
        //sq.array(index, index);
        tail = next_tail;
        // hack
        //tail = 400;
        System.out.println("tail:" + tail);
        sq.releaseTail(tail);
    }
}
