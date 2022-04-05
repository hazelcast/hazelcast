/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

public class JoinPProto<T, S> extends AbstractProcessor {
    /**
     * <p>
     * JOIN condition should be transformed into such form:
     * <pre>
     *  l.time >= r.time - constant1
     *  r.time >= l.time - constant2
     * </pre>
     */
    private final BiPredicateEx<T, S> joinCondition;
    private final ToLongFunctionEx<T> leftTimestampExtractor;
    private final ToLongFunctionEx<S> rightTimestampExtractor;
    private final Map<Byte, Long>[] postponeTimeMap;

    private final PriorityQueue<T> leftBuffer = new PriorityQueue<>();
    private final PriorityQueue<S> rightBuffer = new PriorityQueue<>();

    private Iterator<T> leftPos;
    private Iterator<S> rightPos;

    private T leftItem;
    private S rightItem;

    private Tuple2<T, S> pendingLeftOutput;
    private Tuple2<T, S> pendingRightOutput;
    private Watermark pendingWatermark;

    public JoinPProto(
            BiPredicateEx<T, S> joinCondition,
            Map<Byte, Long>[] postponeTimeMap,
            ToLongFunctionEx<T> leftTimestampExtractor,
            ToLongFunctionEx<S> rightTimestampExtractor
    ) {
        this.joinCondition = joinCondition;
        this.postponeTimeMap = postponeTimeMap;
        this.leftTimestampExtractor = leftTimestampExtractor;
        this.rightTimestampExtractor = rightTimestampExtractor;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        if (pendingLeftOutput != null) {
            if (tryEmit(pendingLeftOutput)) {
                pendingLeftOutput = null;
            }
            return false;
        }

        // left input, traverse right buffer
        if (leftItem == null) {
            leftItem = (T) item;
            leftBuffer.offer(leftItem);
            rightPos = rightBuffer.iterator();
        }

        if (!rightPos.hasNext()) {
            leftItem = null;
            return true;
        }

        // TODO: use JOIN condition
        // TODO: tuple -> real join operation.
        pendingLeftOutput = Tuple2.tuple2(leftItem, rightPos.next());
        if (tryEmit(pendingLeftOutput)) {
            pendingLeftOutput = null;
        }
        return false;
    }

    @Override
    protected boolean tryProcess1(@Nonnull Object item) {
        if (pendingRightOutput != null) {
            if (tryEmit(pendingRightOutput)) {
                pendingRightOutput = null;
            }
            return false;
        }

        // right input, traverse left buffer
        if (rightItem == null) {
            rightItem = (S) item;
            rightBuffer.offer(rightItem);
            leftPos = leftBuffer.iterator();
        }

        if (!leftPos.hasNext()) {
            rightItem = null;
            return true;
        }

        // TODO: use JOIN condition
        // TODO: tuple -> real join operation.
        pendingRightOutput = Tuple2.tuple2(leftPos.next(), rightItem);
        if (tryEmit(pendingRightOutput)) {
            pendingRightOutput = null;
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        if (pendingWatermark != null) {
            if (tryEmit(pendingWatermark)) {
                pendingWatermark = null;
            }
            return false;
        }

        byte key = watermark.key();
        long ts = watermark.timestamp();

        // TODO: wm coalescing -- research.
        clearExpiredItemsInBuffer(ordinal, ts - postponeTimeMap[ordinal].get(key));

        if (!tryEmit(watermark)) {
            pendingWatermark = watermark;
            return false;
        }
        return true;
    }

    private void clearExpiredItemsInBuffer(int ordinal, long postponeTime) {
        if (ordinal == 0) {
            clearLeftBuffer(postponeTime);
        } else {
            clearRightBuffer(postponeTime);
        }
    }

    private void clearLeftBuffer(long postponeTime) {
        // since leftBuffer is a priority queue, we may do
        // just linear scan to first non-matched item.
        while (!leftBuffer.isEmpty()) {
            long ts = leftTimestampExtractor.applyAsLong(leftBuffer.peek());
            if (ts <= postponeTime) {
                leftBuffer.poll();
            } else {
                return;
            }
        }
    }

    private void clearRightBuffer(long evictionTimestamp) {
        // since rightBuffer is a priority queue, we may do
        // just linear scan to first non-matched item.
        while (!rightBuffer.isEmpty()) {
            long ts = rightTimestampExtractor.applyAsLong(rightBuffer.peek());
            if (ts <= evictionTimestamp) {
                rightBuffer.poll();
            } else {
                return;
            }
        }
    }
}