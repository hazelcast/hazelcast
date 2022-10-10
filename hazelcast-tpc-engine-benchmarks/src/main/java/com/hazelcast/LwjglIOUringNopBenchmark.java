package com.hazelcast;

import com.hazelcast.internal.tpc.iouring.CompletionQueue;
import com.hazelcast.internal.tpc.iouring.IOCompletionHandler;
import com.hazelcast.internal.tpc.iouring.IOUring;
import com.hazelcast.internal.tpc.iouring.Linux;
import com.hazelcast.internal.tpc.iouring.SubmissionQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;

public class LwjglIOUringNopBenchmark {
    public static boolean spin = true;
    public static int concurrency = 64;
    public static long operations = 1000 * 1000 * 1000;

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        long startTimeMs = System.currentTimeMillis();
        new WorkerThread(latch).start();
        latch.await();
        long duration = System.currentTimeMillis() - startTimeMs;
        System.out.println((operations * 1000 / duration) + " IOPS");
    }

    private static class WorkerThread extends Thread {

        private final CountDownLatch latch;

        public WorkerThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            IOUring uring = new IOUring(4096, 0);
            SubmissionQueue sq = uring.getSubmissionQueue();
            CompletionQueue cq = uring.getCompletionQueue();

            CompletionHandler handler = new CompletionHandler(sq, latch);

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
