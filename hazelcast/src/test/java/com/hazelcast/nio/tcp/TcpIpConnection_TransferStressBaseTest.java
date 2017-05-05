/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.ChannelReader;
import com.hazelcast.internal.networking.ChannelWriter;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestThread;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/**
 * This test will concurrently write to a single connection and check if all the data transmitted, is received
 * on the other side.
 * <p/>
 * In the past we had some issues with packet not getting written. So this test will write various size packets (from small
 * to very large).
 */
public abstract class TcpIpConnection_TransferStressBaseTest extends TcpIpConnection_AbstractTest {

    // total running time for writer threads
    private static final long WRITER_THREAD_RUNNING_TIME_IN_SECONDS = TimeUnit.MINUTES.toSeconds(2);
    // maximum number of pending packets
    private static final int maxPendingPacketCount = 10000;
    // we create the payloads up front and select randomly from them. This is the number of payloads we are creating
    private static final int payloadCount = 10000;

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private DummyPayload[] payloads;

    @Before
    public void setup() throws Exception {
        super.setup();
        startAllConnectionManagers();
    }

    @Test
    public void testTinyPackets() throws Exception {
        makePayloads(10);
        testPackets();
    }

    @Test
    public void testSmallPackets() throws Exception {
        makePayloads(100);
        testPackets();
    }

    @Test
    public void testMediumPackets() throws Exception {
        makePayloads(1000);
        testPackets();
    }

    @Test(timeout = 10 * 60 * 1000)
    public void testLargePackets() throws Exception {
        makePayloads(10000);
        testPackets((10 * 60 * 1000) - (WRITER_THREAD_RUNNING_TIME_IN_SECONDS * 1000));
    }

    @Test
    public void testSemiRealisticPackets() throws Exception {
        makeSemiRealisticPayloads();
        testPackets();
    }

    private void testPackets() throws Exception {
        testPackets(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private void testPackets(long verifyTimeoutInMillis) throws Exception {
        TcpIpConnection c = connect(connManagerA, addressB);

        WriteThread thread1 = new WriteThread(1, c);
        WriteThread thread2 = new WriteThread(2, c);

        logger.info("Starting");
        thread1.start();
        thread2.start();

        sleepAndStop(stop, WRITER_THREAD_RUNNING_TIME_IN_SECONDS);

        logger.info("Done");

        thread1.assertSucceedsEventually();
        thread2.assertSucceedsEventually();

        // there is always one packet extra for the bind-request
        final long expectedNormalPackets = thread1.normalPackets + thread2.normalPackets + 1;
        final long expectedUrgentPackets = thread1.urgentPackets + thread2.urgentPackets;

        logger.info("expected normal packets: " + expectedNormalPackets);
        logger.info("expected priority packets: " + expectedUrgentPackets);

        final ChannelWriter writer = c.getChannelWriter();
        final ChannelReader reader = ((TcpIpConnection) connManagerB.getConnection(addressA)).getChannelReader();
        long start = System.currentTimeMillis();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logger.info("writer total frames pending   : " + writer.totalFramesPending());
                logger.info("writer last write time millis : " + writer.lastWriteTimeMillis());

                logger.info("reader total frames handled   : " + reader.getNormalFramesReadCounter().get()
                        + reader.getPriorityFramesReadCounter().get());
                logger.info("reader last read time millis  : " + reader.lastReadTimeMillis());

                assertEquals(expectedNormalPackets, reader.getNormalFramesReadCounter().get());
                assertEquals(expectedUrgentPackets, reader.getPriorityFramesReadCounter().get());
            }
        }, verifyTimeoutInMillis);
        logger.info("Waiting for pending packets to be sent and received finished in "
                + (System.currentTimeMillis() - start) + " milliseconds");
    }

    private void makePayloads(int maxSize) {
        Random random = new Random();
        payloads = new DummyPayload[payloadCount];

        for (int k = 0; k < payloads.length; k++) {
            final byte[] bytes = new byte[random.nextInt(maxSize)];
            payloads[k] = new DummyPayload(bytes, false);
        }
    }

    private void makeSemiRealisticPayloads() {
        Random random = new Random();
        payloads = new DummyPayload[payloadCount];

        for (int k = 0; k < payloads.length; k++) {
            // one in every 100 packet we make big
            boolean largePacket = random.nextInt(100) == 1;
            boolean ultraLarge = false;
            if (largePacket) {
                ultraLarge = random.nextInt(10) == 1;
            }

            int byteCount;
            if (ultraLarge) {
                byteCount = random.nextInt(100000);
            } else if (largePacket) {
                byteCount = random.nextInt(10000);
            } else {
                byteCount = random.nextInt(300);
            }

            // one in every 100 packet we make urgent
            boolean urgentPacket = random.nextInt(100) == 1;

            payloads[k] = new DummyPayload(new byte[byteCount], urgentPacket);
        }
    }

    public class WriteThread extends TestThread {

        private final Random random = new Random();
        private final ChannelWriter writeHandler;
        private long normalPackets;
        private long urgentPackets;

        public WriteThread(int id, TcpIpConnection c) {
            super("WriteThread-" + id);
            this.writeHandler = c.getChannelWriter();
        }

        @Override
        public void doRun() throws Throwable {
            long prev = System.currentTimeMillis();

            while (!stop.get()) {
                Packet packet = nextPacket();
                if (packet.isUrgent()) {
                    urgentPackets++;
                } else {
                    normalPackets++;
                }
                writeHandler.write(packet);

                long now = System.currentTimeMillis();
                if (now > prev + 2000) {
                    prev = now;
                    logger.info("At normal-packets:" + normalPackets + " priority-packets:" + urgentPackets);
                }

                double usage = getUsage();
                if (usage < 90) {
                    continue;
                }

                for (; ; ) {
                    sleep(random.nextInt(5));
                    if (getUsage() < 10 || stop.get()) {
                        break;
                    }
                }
            }

            logger.info("Finished, normal packets written: " + normalPackets
                    + " urgent packets written:" + urgentPackets
                    + " total frames pending:" + writeHandler.totalFramesPending());
        }

        private double getUsage() {
            return 100d * writeHandler.totalFramesPending() / maxPendingPacketCount;
        }

        public Packet nextPacket() {
            DummyPayload payload = payloads[random.nextInt(payloads.length)];
            Packet packet = new Packet(serializationService.toBytes(payload));
            if (payload.isUrgent()) {
                packet.raiseFlags(Packet.FLAG_URGENT);
            }
            return packet;
        }

    }

}
