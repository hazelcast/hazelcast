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

import com.hazelcast.internal.tpcengine.FormatUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_NOP;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Does a NopBenchmark directly on the uring without the overhead
 * of the eventloop. It will help to figure out how much performance
 * is lost due to the UringEventloop/AsyncFile etc infrastructure.
 */
public class UringNopBenchmark {
    public boolean spin = false;
    public int concurrency = 1;
    public int runtimeSeconds = 30;
    private volatile boolean stop;
    private final AtomicLong count = new AtomicLong();

    public static void main(String[] args) throws InterruptedException {
        UringNopBenchmark benchmark = new UringNopBenchmark();
        benchmark.run();
    }

    private void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        long startTimeMs = System.currentTimeMillis();
        MonitorThread monitorThread = new MonitorThread();
        monitorThread.start();
        new WorkerThread(latch).start();

        latch.await();
        long duration = System.currentTimeMillis() - startTimeMs;
        System.out.println((count.get() * 1000 / duration) + " NOPS");
        System.exit(0);
    }

    private class WorkerThread extends Thread {

        private final CountDownLatch latch;

        public WorkerThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                final Uring uring = new Uring(4096, 0);
                final SubmissionQueue sq = uring.submissionQueue();
                final CompletionQueue cq = uring.completionQueue();
                final NopCompletionHandler handler = new NopCompletionHandler(sq, latch);
                final boolean spin = UringNopBenchmark.this.spin;

                for (int k = 0; k < concurrency; k++) {
                    sq.prepare(IORING_OP_NOP, 0, 0, 0, 0, 0, 0, 0);
                }

                while (!stop) {
                    if (spin) {
                        sq.submit();
                    } else {
                        sq.submitAndWait();
                    }
                    int cnt = cq.process(handler);
                    count.lazySet(count.get() + cnt);
                }

                latch.countDown();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static class NopCompletionHandler implements CompletionQueue.CompletionHandler {
        private final SubmissionQueue sq;
        private final CountDownLatch latch;
        private long iteration = 0;

        public NopCompletionHandler(SubmissionQueue sq, CountDownLatch latch) {
            this.sq = sq;
            this.latch = latch;
        }

        @Override
        public void complete(int res, int flags, long userdata) {
            if (res < 0) {
                throw new UncheckedIOException(new IOException(Linux.strerror(-res)));
            }

            sq.prepare(IORING_OP_NOP, 0, 0, 0, 0, 0, 0, 0);
        }
    }

    private class MonitorThread extends Thread {

        public MonitorThread() {
            super("MonitorThread");
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            stop = true;
        }

        private void run0() throws Exception {
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            long lastMs = startMs;

            StringBuffer sb = new StringBuffer();
            Metrics lastMetrics = new Metrics();
            Metrics metrics = new Metrics();
            while (System.currentTimeMillis() < endMs) {
                Thread.sleep(1000);

                long nowMs = System.currentTimeMillis();
                long durationMs = nowMs - lastMs;
                collect(metrics);

                long completedMs = TimeUnit.MILLISECONDS.toSeconds(nowMs - startMs);
                long completedMinutes = completedMs / 60;
                long completedSeconds = completedMs % 60;

                double completed = (100f * completedMs) / runtimeMs;
                sb.append("  [etd ");
                sb.append(completedMinutes);
                sb.append("m:");
                sb.append(completedSeconds);
                sb.append("s ");
                sb.append(String.format("%,.3f", completed));
                sb.append("%]");

                long eta = TimeUnit.MILLISECONDS.toSeconds(endMs - nowMs);
                long etaMinutes = eta / 60;
                long etaSeconds = eta % 60;
                sb.append("[eta ");
                sb.append(etaMinutes);
                sb.append("m:");
                sb.append(etaSeconds);
                sb.append("s]");

                long count = metrics.count;
                double readsThp = ((count - lastMetrics.count) * 1000d) / durationMs;
                sb.append("[cnt=");
                sb.append(FormatUtil.humanReadableCountSI(readsThp));
                sb.append("/s]");

                System.out.println(sb);
                sb.setLength(0);

                Metrics tmp = lastMetrics;
                lastMetrics = metrics;
                metrics = tmp;
                lastMs = nowMs;
            }
        }
    }

    private static class Metrics {
        public long count;

        private void clear() {
            this.count = count;
        }
    }

    private void collect(Metrics target) {
        target.clear();

        target.count = UringNopBenchmark.this.count.get();
    }
}
