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

import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

/**
 * This is an endurance test that tests if the uring wrapper can correctly
 * handle overflow problems. The issue is that the C implementation of uring
 * uses a 32 bit unsigned 32 bits int and Java doesn't support that explicitly
 * because in Java all ints are signed.
 * <p/>
 * So this test will run concurrent nops on a uring instances and checks if
 * there are any problems and all the issues requests complete without errors.
 * <p/>
 * On a reasonably powerful machine (AMD 5950x) this test takes almost 3m to
 * complete with 10B operations and concurrency of 1000.
 */
@Category(NightlyTest.class)
public class UringEnduranceTest {
    // an Integer.MAX goes to 2.1B and an unsigned (32 bit) goes to 4.3B. So
    // if we run to 10B we should have covered the overflow situations.
    private long operations = 10_000_000_000L;
    // the number of concurrent nops. If this number gets very low, the test will
    // take a lot more time.
    private int concurrency = 1000;
    private final AtomicLong count = new AtomicLong();
    private Uring uring;
    private SubmissionQueue sq;
    private CompletionQueue cq;
    private NopCompletionHandler completionHandler;
    private MonitorThread monitorThread;

    @Before
    public void before() {
        uring = new Uring(nextPowerOfTwo(concurrency), 0);
        sq = uring.submissionQueue();
        cq = uring.completionQueue();
        completionHandler = new NopCompletionHandler();
        monitorThread = new MonitorThread();
        monitorThread.start();
    }

    @After
    public void after() throws InterruptedException {
        if (uring != null) {
            uring.close();
        }
        monitorThread.shutdown();
        monitorThread.join();
    }

    @Test
    public void test() throws InterruptedException {
        long rounds = operations / concurrency;
        long remaining = operations % concurrency;
        if (remaining != 0) {
            fail("operations is not a multiple of the concurrency");
        }

        int handlerId = cq.nextHandlerId();
        cq.register(handlerId, completionHandler);

        for (long round = 0; round < rounds; round++) {
            // offer the requests.
            for (int l = 0; l < concurrency; l++) {
                if (!sq.offer(Uring.IORING_OP_NOP, 0, 0, 0, 0, 0, 0, handlerId)) {
                    fail("failed to offer nop");
                }
            }

            // submit them to the uring; uring will immediately complete them
            sq.submit();

            // process the completion events.
            int cnt = cq.process();
            // make sure that all the submitted nops were received.
            assertEquals(concurrency, cnt);

            if (completionHandler.errors > 0) {
                fail("Errors encountered, nr-errors=" + completionHandler.errors);
            }
        }

        assertEquals(operations, completionHandler.completions);
        assertEquals(0, completionHandler.errors);
    }

    private class MonitorThread extends Thread {
        private volatile boolean stop;

        private MonitorThread() {
            super("MonitorThread");
        }

        private void shutdown() {
            stop = true;
            interrupt();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    continue;
                }

                System.out.println("Completed: " + String.format("%,d", count.get()));
            }
        }
    }

    private class NopCompletionHandler implements CompletionHandler {
        private long completions;
        private long errors;

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            completions++;
            count.lazySet(count.get() + 1);

            if (res < 0) {
                System.out.println(Linux.strerror(-res));
                errors++;
            }
        }
    }
}
