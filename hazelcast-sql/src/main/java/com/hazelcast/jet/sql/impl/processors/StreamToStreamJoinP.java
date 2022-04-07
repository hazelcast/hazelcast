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

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingLong;

public class StreamToStreamJoinP extends AbstractProcessor {
    // TODO: replace with ExpressionEvalContext.from(context) in init() method.
    private static final ExpressionEvalContext MOCK_EEC =
            new ExpressionEvalContext(emptyList(), new DefaultSerializationServiceBuilder().build());
    /**
     * <p>
     * JOIN condition should be transformed into such form:
     * <pre>
     *  l.time >= r.time - constant1
     *  r.time >= l.time - constant2
     * </pre>
     */
    private final JetJoinInfo joinInfo;
    private final ToLongFunctionEx<JetSqlRow> leftTimestampExtractor;
    private final ToLongFunctionEx<JetSqlRow> rightTimestampExtractor;
    private final Map<Byte, Long>[] postponeTimeMap;

    private final PriorityQueue<JetSqlRow> leftBuffer;
    private final PriorityQueue<JetSqlRow> rightBuffer;

    private Iterator<JetSqlRow> leftPos;
    private Iterator<JetSqlRow> rightPos;

    private JetSqlRow leftItem;
    private JetSqlRow rightItem;

    private JetSqlRow pendingLeftOutput;
    private JetSqlRow pendingRightOutput;
    private Watermark pendingWatermark;

    public StreamToStreamJoinP(
            final JetJoinInfo joinInfo,
            final ToLongFunctionEx<JetSqlRow> leftTimestampExtractor,
            final ToLongFunctionEx<JetSqlRow> rightTimestampExtractor,
            final Map<Byte, Long>[] postponeTimeMap
    ) {
        this.joinInfo = joinInfo;
        this.leftTimestampExtractor = leftTimestampExtractor;
        this.rightTimestampExtractor = rightTimestampExtractor;
        this.postponeTimeMap = postponeTimeMap;

        leftBuffer = new PriorityQueue<>(comparingLong(leftTimestampExtractor));
        rightBuffer = new PriorityQueue<>(comparingLong(rightTimestampExtractor));
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
            leftItem = (JetSqlRow) item;
            leftBuffer.offer(leftItem);
            rightPos = rightBuffer.iterator();
        }

        if (!rightPos.hasNext()) {
            leftItem = null;
            return true;
        }

        pendingLeftOutput = ExpressionUtil.join(leftItem, rightPos.next(), joinInfo.condition(), MOCK_EEC);
        if (pendingLeftOutput != null && tryEmit(pendingLeftOutput)) {
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
            rightItem = (JetSqlRow) item;
            rightBuffer.offer(rightItem);
            leftPos = leftBuffer.iterator();
        }

        if (!leftPos.hasNext()) {
            rightItem = null;
            return true;
        }

        pendingRightOutput = ExpressionUtil.join(leftPos.next(), rightItem, joinInfo.condition(), MOCK_EEC);
        if (pendingRightOutput != null && tryEmit(pendingRightOutput)) {
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
        long postponeTime = ts - postponeTimeMap[ordinal].get(key);
        clearExpiredItemsInBuffer(
                ordinal == 0 ? leftBuffer : rightBuffer,
                postponeTime,
                ordinal == 0 ? leftTimestampExtractor : rightTimestampExtractor);

        if (!tryEmit(watermark)) {
            pendingWatermark = watermark;
            return false;
        }
        return true;
    }

    private void clearExpiredItemsInBuffer(
            PriorityQueue<JetSqlRow> buffer,
            long timeLimit,
            ToLongFunctionEx<JetSqlRow> tsExtractor) {
        // since buffer is a priority queue, we may do just linear scan to first non-matched item.
        while (!buffer.isEmpty()) {
            long ts = tsExtractor.applyAsLong(buffer.peek());
            if (ts <= timeLimit) {
                buffer.poll();
            } else {
                return;
            }
        }
    }
}
