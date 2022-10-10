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


public class HighLevelMain {

    public static void main(String[] args) throws InterruptedException {
        IOUring uring = new IOUring(16384, 0);

        SubmissionQueue sq = uring.getSubmissionQueue();

        int count = 10;
        for (int k = 0; k < count; k++) {
            sq.offerNop(10);
        }
        int submittedEntries = sq.submit();

        System.out.println("submittedEntries:" + submittedEntries);
        CompletionQueue cq = uring.getCompletionQueue();
        int head = cq.acquireHead();// no acquire needed

        for (; ; ) {
            int tail = cq.acquireTail();
            if (head == tail) {
                // ringbuffer is empty
                break;
            }

            int index = head & cq.ringMask();
            System.out.println("completion index:" + index);
            head++;
        }
        cq.releaseHead(head);
    }
}