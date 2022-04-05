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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

public class JoinPProto extends AbstractProcessor {
    private final BiPredicateEx<Long, Long> predicate;
    private final Map<Byte, Long> postponeTimeMap = new HashMap<>();
    private final long postponeTimeDiff;

    private final PriorityQueue<Long> leftBuffer = new PriorityQueue<>();
    private final PriorityQueue<Long> rightBuffer = new PriorityQueue<>();

    private Iterator<Long> leftPos;
    private Iterator<Long> rightPos;

    private Long leftItem;
    private Long rightItem;

    private Tuple2<Long, Long> pendingLeftOutput;
    private Tuple2<Long, Long> pendingRightOutput;
    private Watermark pendingWatermark;

    private long lastLeftWatermark = Long.MIN_VALUE;
    private long lastRightWatermark = Long.MIN_VALUE;

    public JoinPProto() {
        this.predicate = (left, right) -> right >= left - 2L;
        this.postponeTimeDiff = 2L;
    }

    public JoinPProto(BiPredicateEx<Long, Long> predicate, long postponeTimeDiff) {
        this.predicate = predicate;
        this.postponeTimeDiff = postponeTimeDiff;
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
            leftItem = (Long) item;
            leftBuffer.offer(leftItem);
            rightPos = rightBuffer.iterator();
        }

        if (!rightPos.hasNext()) {
            leftItem = null;
            return true;
        }

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
            rightItem = (Long) item;
            rightBuffer.offer(rightItem);
            leftPos = leftBuffer.iterator();
        }

        if (!leftPos.hasNext()) {
            rightItem = null;
            return true;
        }

        pendingRightOutput = Tuple2.tuple2(leftPos.next(), rightItem);
        if (tryEmit(pendingRightOutput)) {
            pendingRightOutput = null;
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (pendingWatermark != null) {
            if (tryEmit(pendingWatermark)) {
                pendingWatermark = null;
            }
            return false;
        }

        byte key = watermark.key();
        long wm = watermark.timestamp();

        if (key == 0) {
            if (lastLeftWatermark == Long.MIN_VALUE) {
                lastLeftWatermark = wm;
                postponeTimeMap.put(key, wm + postponeTimeDiff);
                if (!tryEmit(watermark)) {
                    pendingWatermark = watermark;
                    return false;
                } else {
                    return true;
                }
            }
            clearExpiredBufferEvents(leftBuffer, postponeTimeMap.get(key));
            postponeTimeMap.put(key, lastLeftWatermark);
            lastLeftWatermark = wm;
        } else {
            if (lastRightWatermark == Long.MIN_VALUE) {
                lastRightWatermark = wm;
                postponeTimeMap.put(key, wm + postponeTimeDiff);
                if (!tryEmit(watermark)) {
                    pendingWatermark = watermark;
                    return false;
                } else {
                    return true;
                }
            }
            clearExpiredBufferEvents(rightBuffer, postponeTimeMap.get(key));
            postponeTimeMap.put(key, wm + postponeTimeDiff);
            lastRightWatermark = wm;
        }

        // TODO: wm coalescing -- research.
        if (!tryEmit(watermark)) {
            pendingWatermark = watermark;
        } else {
            return true;
        }
        return false;
    }

    private void clearExpiredBufferEvents(PriorityQueue<Long> buffer, Long postponeTime) {
        while (!buffer.isEmpty() && buffer.peek() <= postponeTime) {
            buffer.poll();
        }
    }
}
