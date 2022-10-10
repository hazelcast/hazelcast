package io.netty.incubator.channel.uring;

import com.hazelcast.internal.tpc.iouring.IOUring;
import com.hazelcast.internal.tpc.iouring.Linux;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;

public class NettyNopBenchmark {
    public static boolean spin = true;
    public static int concurrency = 64;
    public static long operations = 100 * 1000 * 1000;

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
                RingBuffer uring = Native.createRingBuffer(4096);
                IOUringSubmissionQueue sq = uring.ioUringSubmissionQueue();
                IOUringCompletionQueue cq = uring.ioUringCompletionQueue();

                CompletionHandler handler = new CompletionHandler(sq, latch);

                for (int k = 0; k < concurrency; k++) {
                    sq.enqueueSqe(IOUring.IORING_OP_NOP, 0, 0, 0, 0, 0, 0, (short) 0);
                }

                for (; ; ) {
                    if (spin) {
                        sq.submit();
                    } else {
                        sq.submitAndWait();
                    }
                    cq.process(handler);
                }
            }catch (Throwable t){
                t.printStackTrace();
            }
        }
    }

    private static class CompletionHandler implements IOUringCompletionQueueCallback {
        private final IOUringSubmissionQueue sq;
        private final CountDownLatch latch;
        private long iteration = 0;

        public CompletionHandler(IOUringSubmissionQueue sq, CountDownLatch latch) {
            this.sq = sq;
            this.latch = latch;
        }

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            if (res < 0) {
                throw new UncheckedIOException(new IOException(Linux.strerror(-res)));
            }

            iteration++;
            if (iteration == operations) {
                latch.countDown();
            } else {
                sq.enqueueSqe(IOUring.IORING_OP_NOP, 0, 0, 0, 0, 0, 0, (short) 0);
            }
        }
    }
}
