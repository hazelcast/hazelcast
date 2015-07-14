/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private static final long TIMEOUT = 3;

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final Queue<SocketWritable> urgentWriteQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final ByteBuffer outputBuffer;
    private SocketWritable currentPacket;
    private SocketWriter socketWriter;
    private volatile long lastHandle;
    //This field will be incremented by a single thread. It can be read by multiple threads.
    private volatile long eventCount;
    private boolean shutdown;
    // this field will be accessed by the IOSelector-thread or
    // it is accessed by any other thread but only that thread managed to cas the scheduled flag to true.
    // This prevents running into an IOSelector that is migrating.
    private IOSelector newOwner;

    WriteHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection, ioSelector, SelectionKey.OP_WRITE);
        this.outputBuffer = ByteBuffer.allocate(connectionManager.socketSendBufferSize);
    }

    long getLastHandle() {
        return lastHandle;
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
    }

    // accessed from ReadHandler and SocketConnector
    void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioSelector.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                createWriter(protocol);
                latch.countDown();
            }
        });
        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    private void createWriter(String protocol) {
        if (socketWriter == null) {
            if (Protocols.CLUSTER.equals(protocol)) {
                socketWriter = new SocketPacketWriter(connection);
                outputBuffer.put(stringToBytes(Protocols.CLUSTER));
                registerOp(SelectionKey.OP_WRITE);
            } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                socketWriter = new SocketClientDataWriter();
            } else if (Protocols.CLIENT_BINARY_NEW.equals(protocol)) {
                socketWriter = new SocketClientMessageWriter();
            } else {
                socketWriter = new SocketTextWriter(connection);
            }
        }
    }

    public void offer(SocketWritable packet) {
        if (packet.isUrgent()) {
            urgentWriteQueue.offer(packet);
        } else {
            writeQueue.offer(packet);
        }

        schedule();
    }

    private SocketWritable poll() {
        for (; ; ) {
            SocketWritable packet = urgentWriteQueue.poll();

            if (packet == null) {
                packet = writeQueue.poll();
            }

            if (packet instanceof TaskPacket) {
                ((TaskPacket) packet).run();
                continue;
            }

            return packet;
        }
    }

    /**
     * Makes sure this WriteHandler is scheduled to be executed by the IO thread.
     * <p/>
     * This call is made by 'outside' threads that interact with the connection. For example when a packet is placed
     * on the connection to be written. It will never be made by an IO thread.
     * <p/>
     * If the WriteHandler already is scheduled, the call is ignored.
     */
    private void schedule() {
        if (scheduled.get()) {
            // So this WriteHandler is still scheduled, we don't need to schedule it again
            return;
        }

        if (!scheduled.compareAndSet(false, true)) {
            // Another thread already has scheduled this WriteHandler, we are done. It
            // doesn't matter which thread does the scheduling, as long as it happens.
            return;
        }

        // We managed to schedule this WriteHandler. This means we need to add a task to
        // the ioReactor and to give the reactor-thread a kick so that it processes our packets.
        ioSelector.addTaskAndWakeup(this);
    }

    /**
     * Tries to unschedule this WriteHandler.
     * <p/>
     * It will only be unscheduled if:
     * - the outputBuffer is empty
     * - there are no pending packets.
     * <p/>
     * If the outputBuffer is dirty then it will register itself for an OP_WRITE since we are interested in knowing
     * if there is more space in the socket output buffer.
     * If the outputBuffer is not dirty, then it will unregister itself from an OP_WRITE since it isn't interested
     * in space in the socket outputBuffer.
     * <p/>
     * This call is only made by the IO thread.
     */
    private void unschedule() {
        if (dirtyOutputBuffer() || currentPacket != null) {
            // Because not all data was written to the socket, we need to register for OP_WRITE so we get
            // notified when the socketChannel is ready for more data.
            registerOp(SelectionKey.OP_WRITE);

            // If the outputBuffer is not empty, we don't need to unschedule ourselves. This is because the
            // WriteHandler will be triggered by a nio write event to continue sending data.
            return;
        }

        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(SelectionKey.OP_WRITE);
        // So the outputBuffer is empty, so we are going to unschedule ourselves.
        scheduled.set(false);

        if (writeQueue.isEmpty() && urgentWriteQueue.isEmpty()) {
            // there are no remaining packets, so we are done.
            return;
        }

        // So there are packet, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Packets are at risk not to be send.

        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this WriteHandler, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the ioSelector so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread and the selectionQueue will be processed
        // till it is empty. So it will also pick up tasks that are added while it is processing the selectionQueue.
        ioSelector.addTask(this);
    }

    @Override
    public long getEventCount() {
        return eventCount;
    }

    @Override
    @SuppressWarnings("unchecked")
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "eventCount is accessed by a single thread only.")
    public void handle() {
        eventCount++;
        lastHandle = Clock.currentTimeMillis();
        if (shutdown) {
            return;
        }

        if (socketWriter == null) {
            logger.log(Level.WARNING, "SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            createWriter(Protocols.CLUSTER);
        }

        try {
            fillOutputBuffer();

            if (dirtyOutputBuffer()) {
                writeOutputBufferToSocket();
            }
        } catch (Throwable t) {
            logger.severe("Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
        }

        if (newOwner == null) {
            unschedule();
        } else {
            IOSelector newOwner = this.newOwner;
            this.newOwner = null;
            startMigration(newOwner);
        }
    }

    /**
     * Checks of the outputBuffer is dirty.
     *
     * @return true if dirty, false otherwise.
     */
    private boolean dirtyOutputBuffer() {
        return outputBuffer.position() > 0;
    }

    /**
     * Writes to content of the outputBuffer to the socket.
     *
     * @throws Exception
     */
    private void writeOutputBufferToSocket() throws Exception {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
        outputBuffer.flip();
        try {
            socketChannel.write(outputBuffer);
        } catch (Exception e) {
            currentPacket = null;
            handleSocketException(e);
            return;
        }
        // Now we verify if all data is written.
        if (!outputBuffer.hasRemaining()) {
            // We managed to fully write the outputBuffer to the socket, so we are done.
            outputBuffer.clear();
            return;
        }
        // We did not manage to write all data to the socket. So lets compact the buffer so new data
        // can be added at the end.
        outputBuffer.compact();
    }

    /**
     * Fills the outBuffer with packets. This is done till there are no more packets or till there is no more space in the
     * outputBuffer.
     *
     * @throws Exception
     */
    private void fillOutputBuffer() throws Exception {
        for (; ; ) {
            if (!outputBuffer.hasRemaining()) {
                // The buffer is completely filled, we are done.
                return;
            }

            // If there currently is not packet sending, lets try to get one.
            if (currentPacket == null) {
                currentPacket = poll();
                if (currentPacket == null) {
                    // There is no packet to write, we are done.
                    return;
                }
            }

            // Lets write the currentPacket to the outputBuffer.
            if (!socketWriter.write(currentPacket, outputBuffer)) {
                // We are done for this round because not all data of the current packet fits in the outputBuffer
                return;
            }

            // The current packet has been written completely. So lets null it and lets try to write another packet.
            currentPacket = null;
        }
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (Throwable e) {
            ioSelector.handleSelectionKeyFailure(e);
        }
    }

    public void shutdown() {
        writeQueue.clear();
        urgentWriteQueue.clear();

        ShutdownTask shutdownTask = new ShutdownTask();
        offer(shutdownTask);
        shutdownTask.awaitCompletion();
    }

    @Override
    public void requestMigration(IOSelector newOwner) {
        offer(new StartMigrationTask(newOwner));
    }

    @Override
    public String toString() {
        return connection + ".writeHandler";
    }

    /**
     * The taskPacket is not really a Packet. It is a way to put a task on one of the packet queues. Using this approach we
     * can lift on top of the packet scheduling mechanism and we can prevent having:
     * - multiple IOSelector-tasks for a WriteHandler on multiple IOSelectors
     * - multiple IOSelector-tasks for a WriteHandler on the same IOSelector.
     */
    private abstract class TaskPacket implements SocketWritable {
        abstract void run();

        @Override
        public boolean writeTo(ByteBuffer destination) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isUrgent() {
            return true;
        }
    }

    /**
     * Triggers the migration when executed by setting the WriteHandler.newOwner field. When the handle method completes, it
     * checks if this field if set, if so, the migration starts.
     *
     * If the current ioSelector is the same as 'theNewOwner' then the call is ignored.
     */
    private class StartMigrationTask extends TaskPacket {
        // field is called 'theNewOwner' to prevent any ambiguity problems with the writeHandler.newOwner.
        // Else you get a lot of ugly WriteHandler.this.newOwner is ...
        private final IOSelector theNewOwner;

        public StartMigrationTask(IOSelector theNewOwner) {
            this.theNewOwner = theNewOwner;
        }

        @Override
        void run() {
            assert newOwner == null : "No migration can be in progress";

            if (ioSelector == theNewOwner) {
                // if there is no change, we are done
                return;
            }

            newOwner = theNewOwner;
        }
    }

    private class ShutdownTask extends TaskPacket {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        void run() {
            shutdown = true;
            try {
                socketChannel.closeOutbound();
            } catch (IOException e) {
                logger.finest("Error while closing outbound", e);
            } finally {
                latch.countDown();
            }
        }

        void awaitCompletion() {
            try {
                latch.await(TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }
}
