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

package com.hazelcast.iouring;

import com.hazelcast.internal.tpcengine.iouring.CompletionQueue;
import com.hazelcast.internal.tpcengine.iouring.IOUring;
import com.hazelcast.internal.tpcengine.iouring.Linux;
import com.hazelcast.internal.tpcengine.iouring.SubmissionQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;

public class IOUringNopBenchmark {
    public static final boolean spin = true;
    public static final int concurrency = 64;
    public static final long operations = 1000 * 1000 * 1000;

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        long startTimeMs = System.currentTimeMillis();
        new WorkerThread(latch).start();
        latch.await();
        long duration = System.currentTimeMillis() - startTimeMs;
        System.out.println((operations * 1000 / duration) + " IOPS");
        System.exit(0);
    }

    private static class WorkerThread extends Thread {

        private final CountDownLatch latch;

        public WorkerThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                final IOUring uring = new IOUring(4096, 0);
                final SubmissionQueue sq = uring.submissionQueue();
                final CompletionQueue cq = uring.completionQueue();
                final IOUringNopBenchmark.CompletionHandler handler = new IOUringNopBenchmark.CompletionHandler(sq, latch);
                final boolean spin = IOUringNopBenchmark.spin;

                for (int k = 0; k < concurrency; k++) {
                    sq.offer(IOUring.IORING_OP_NOP, 0, 0, 0, 0, 0, 0, 0);
                }

                for (; ; ) {
                    if (spin) {
                        sq.submit();
                    } else {
                        sq.submitAndWait();
                    }
                    cq.process(handler);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static class CompletionHandler implements com.hazelcast.internal.tpcengine.iouring.CompletionHandler {
        private final SubmissionQueue sq;
        private final CountDownLatch latch;
        private long iteration = 0;

        public CompletionHandler(SubmissionQueue sq, CountDownLatch latch) {
            this.sq = sq;
            this.latch = latch;
        }

        @Override
        public void handle(int res, int flags, long userdata) {
            if (res < 0) {
                throw new UncheckedIOException(new IOException(Linux.strerror(-res)));
            }

            iteration++;
            if (iteration == operations) {
                latch.countDown();
            } else {
                if (!sq.offer(IOUring.IORING_OP_NOP, 0, 0, 0, 0, 0, 0, 0)) {
                    throw new UncheckedIOException(new IOException("failed to offer"));
                }
            }
        }
    }
}
