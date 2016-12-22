/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Javadoc pending
 */
public class EdgeConfig implements Serializable {

    /**
     * Javadoc pending
     */
    public static final int DEFAULT_HIGH_WATER_MARK = 2048;

    /**
     * Javadoc pending
     */
    public static final int DEFAULT_QUEUE_SIZE = 1024;

    /**
     * Javadoc pending
     */
    public static final int DEFAULT_RECEIVE_WINDOW_MULTIPLIER = 3;

    /**
     * Javadoc pending
     */
    public static final int DEFAULT_PACKET_SIZE_LIMIT = 1 << 14;

    private int highWaterMark = DEFAULT_HIGH_WATER_MARK;
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int receiveWindowMultiplier = DEFAULT_RECEIVE_WINDOW_MULTIPLIER;
    private int packetSizeLimit = DEFAULT_PACKET_SIZE_LIMIT;

    /**
     *
     */
    public EdgeConfig() {
    }

    /**
     * @return
     */
    public int getReceiveWindowMultiplier() {
        return receiveWindowMultiplier;
    }

    /**
     * Javadoc pending
     *
     * @param receiveWindowMultiplier
     * @return
     */
    public EdgeConfig setReceiveWindowMultiplier(int receiveWindowMultiplier) {
        this.receiveWindowMultiplier = receiveWindowMultiplier;
        return this;
    }

    /**
     * Javadoc pending
     *
     * @return
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Javadoc pending
     *
     * @param queueSize
     * @return
     */
    public EdgeConfig setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    /**
     * Javadoc pending
     *
     * @return
     */
    public int getHighWaterMark() {
        return highWaterMark;
    }

    /**
     * Javadoc pending
     *
     * @param highWaterMark
     * @return
     */
    public EdgeConfig setHighWaterMark(int highWaterMark) {
        this.highWaterMark = highWaterMark;
        return this;
    }

    /**
     * Javadoc pending
     *
     * @return
     */
    public int getPacketSizeLimit() {
        return packetSizeLimit;
    }

    /**
     * Javadoc pending
     *
     * @param packetSizeLimit
     */
    public void setPacketSizeLimit(int packetSizeLimit) {
        this.packetSizeLimit = packetSizeLimit;
    }
}
