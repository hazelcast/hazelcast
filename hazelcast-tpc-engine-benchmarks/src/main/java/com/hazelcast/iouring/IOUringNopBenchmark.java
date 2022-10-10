package com.hazelcast.iouring;

import com.hazelcast.internal.tpc.iouring.CompletionQueue;
import com.hazelcast.internal.tpc.iouring.IOCompletionHandler;
import com.hazelcast.internal.tpc.iouring.IOUring;
import com.hazelcast.internal.tpc.iouring.Linux;
import com.hazelcast.internal.tpc.iouring.SubmissionQueue;

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
                final SubmissionQueue sq = uring.getSubmissionQueue();
                final CompletionQueue cq = uring.getCompletionQueue();
                final CompletionHandler handler = new CompletionHandler(sq, latch);
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

    private static class CompletionHandler implements IOCompletionHandler {
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
