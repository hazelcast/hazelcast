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

package com.hazelcast.jet.config;

import com.hazelcast.jet.Edge;

import java.io.Serializable;

/**
 * The configuration object for a DAG {@link Edge}.
 */
public class EdgeConfig implements Serializable {

    /**
     * The default value of the {@link #setOutboxCapacity(int) outbox bucket capacity}.
     */
    public static final int DEFAULT_OUTBOX_CAPACITY = 2048;

    /**
     * The default size of the {@link #setQueueSize(int) concurrent queues}
     * connecting processors within a cluster member.
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

    private int outboxCapacity = DEFAULT_OUTBOX_CAPACITY;
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int receiveWindowMultiplier = DEFAULT_RECEIVE_WINDOW_MULTIPLIER;
    private int packetSizeLimit = DEFAULT_PACKET_SIZE_LIMIT;

    /**
     * Sets the capacity of processor-to-processor concurrent queues.
     * <p>
     * When data needs to travel between two processors on the same cluster member,
     * it is sent over a concurrent single-producer, single-consumer (SPSC) queue of
     * fixed capacity.
     * <p>
     * Since there are several processors executing the logic of each vertex, and
     * since the queues are SPSC, there will be
     * {@code senderParallelism * receiverParallelism} queues representing the edge
     * on each member. Care should be taken to strike a balance between performance
     * and memory usage. The default of {@value #DEFAULT_QUEUE_SIZE} is a good size
     * for simple DAGs and moderate parallelism, but the optimum can be detemined only
     * by experiment.
     */
    public EdgeConfig setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    /**
     * Returns the size of the SPSC queues used to implement this edge.
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Sets the capacity of the outbox bucket corresponding to this edge.
     * <p>
     * A cooperative processor's {@code Outbox} will contain a bucket dedicated
     * to this edge. When the bucket reaches the configured capacity, it will
     * refuse further items. At that time the processor must yield control back
     * to its caller.
     * <p>
     * The default value is {@value #DEFAULT_OUTBOX_CAPACITY}.
     */
    public EdgeConfig setOutboxCapacity(int capacity) {
        this.outboxCapacity = capacity;
        return this;
    }

    /**
     * Returns the {@link #setOutboxCapacity(int) capacity} of the {@code Outbox}
     * bucket corresponding to this edge.
     */
    public int getOutboxCapacity() {
        return outboxCapacity;
    }

    /**
     * Sets the scaling factor used by the adaptive receive window sizing
     * function.
     * <p>
     * For each distributed edge the receiving member regularly sends
     * flow-control ("ack") packets to its sender which prevent it from sending
     * too much data and overflowing the buffers. The sender is allowed to send
     * the data one <em>receive window</em> further than the last acknowledged
     * byte and the receive window is sized in proportion to the rate of
     * processing at the receiver.
     * <p>
     * Ack packets are sent in {@link InstanceConfig#setFlowControlPeriodMs(int)
     * regular intervals} and the <em>receive window multiplier</em> sets the
     * factor of the linear relationship between the amount of data processed
     * within one such interval and the size of the receive window.
     * <p>
     * To put it another way, let us define an <em>ackworth</em> as the amount
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
     * @return the {@link #setReceiveWindowMultiplier(int) receive window multiplier}
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
     * Returns the limit on the {@link #setPacketSizeLimit(int) network packet size},
     * in bytes
     */
    public int getPacketSizeLimit() {
        return packetSizeLimit;
    }
}
