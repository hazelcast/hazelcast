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

package com.hazelcast.jet;

import java.io.Serializable;

/**
 * Configuration properties of a DAG {@link Edge}.
 */
public class EdgeConfig implements Serializable {

    /**
     * The default value of the {@link #setHighWaterMark(int) high water mark}.
     */
    public static final int DEFAULT_HIGH_WATER_MARK = 2048;

    /**
     * The default {@link #setQueueSize(int) SPSC queue size}.
     */
    public static final int DEFAULT_QUEUE_SIZE = 1024;

    /**
     * The default {@link #setReceiveWindowMultiplier(int) receive window multiplier}.
     */
    public static final int DEFAULT_RECEIVE_WINDOW_MULTIPLIER = 3;

    /**
     * The default {@link #setPacketSizeLimit(int) packet size limit}.
     */
    public static final int DEFAULT_PACKET_SIZE_LIMIT = 1 << 14;

    private int highWaterMark = DEFAULT_HIGH_WATER_MARK;
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int receiveWindowMultiplier = DEFAULT_RECEIVE_WINDOW_MULTIPLIER;
    private int packetSizeLimit = DEFAULT_PACKET_SIZE_LIMIT;

    /**
     * When data needs to travel between two processors on the same cluster member,
     * it is sent over a concurrent single-producer, single-consumer (SPSC) queue of
     * fixed size. This method sets the size of the queue.
     * <p>
     * Since there are several processors executing the logic of each vertex, and
     * since the queues are SPSC, there will be
     * {@code senderParallelism * receiverParallelism} queues representing the edge
     * on each member. Care should be taken to strike a balance between performance
     * and memory usage. The default of {@value #DEFAULT_QUEUE_SIZE} is a good size
     * for simple DAGs and moderate parallelism, but the optimum can be detemined only
     * through testing.
     */
    public EdgeConfig setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    /**
     * @return size of the SPSC queues used to implement this edge
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * A {@code Processor} deposits its output items to its {@code Outbox}. It
     * is an unbounded buffer, but has a "high water mark" which should be respected
     * by a well-behaving processor. When its outbox reaches the high water mark,
     * the processor should yield control back to its caller. This method sets
     * the value of the high water mark.
     * <p>
     * The default value is {@value #DEFAULT_HIGH_WATER_MARK}.
     */
    public EdgeConfig setHighWaterMark(int highWaterMark) {
        this.highWaterMark = highWaterMark;
        return this;
    }

    /**
     * @return the high water mark that will be set on the bucket of
     * {@code Outbox} corresponding to this edge
     */
    public int getHighWaterMark() {
        return highWaterMark;
    }

    /**
     * For each distributed edge the receiving member regularly sends flow-control
     * ("ack") packets to its sender which prevent it from sending too much data
     * and overflowing the buffers. The sender is allowed to send the data one
     * <em>receive window</em> further than the last acknowledged byte and the
     * receive window is sized in proportion to the rate of processing at the
     * receiver.
     * <p>
     * Ack packets are sent in regular intervals and the <em>receive window
     * multiplier</em> sets the factor of the linear relationship between the
     * amount of data processed within one such interval and the size of the
     * receive window.
     * <p>
     * To put it another way, let us define an <em>ackworth</em> to be the amount
     * of data processed between two consecutive ack packets. The receive window
     * multiplier determines the number of ackworths the sender can be ahead of
     * the last acked byte.
     * <p>
     * The default value is {@value #DEFAULT_RECEIVE_WINDOW_MULTIPLIER}. This
     * setting has no effect on a non-distributed edge.
     */
    public EdgeConfig setReceiveWindowMultiplier(int receiveWindowMultiplier) {
        this.receiveWindowMultiplier = receiveWindowMultiplier;
        return this;
    }

    /**
     * @return the receive window multiplier
     */
    public int getReceiveWindowMultiplier() {
        return receiveWindowMultiplier;
    }

    /**
     * For a distributed edge, data is sent to a remote member via Hazelcast network
     * packets. Each packet is dedicated to the data of a single edge, but may contain
     * any number of data items. This setting limits the size of the packet in bytes.
     * Packets should be large enough to drown out any fixed overheads, but small enough
     * to allow good interleaving with other packets.
     * <p>
     * Note that a single item cannot straddle packets, therefore the maximum packet size
     * can exceed the value configured here by the size of a single data item.
     * <p>
     * The default value is {@value #DEFAULT_PACKET_SIZE_LIMIT}. This setting has no effect
     * on a non-distributed edge.
     */
    public EdgeConfig setPacketSizeLimit(int packetSizeLimit) {
        this.packetSizeLimit = packetSizeLimit;
        return this;
    }

    /**
     * @return the limit on the network packet size, in bytes
     */
    public int getPacketSizeLimit() {
        return packetSizeLimit;
    }
}
