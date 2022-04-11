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
import com.hazelcast.jet.impl.execution.WatermarkCoalescer;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    private final PriorityQueue<JetSqlRow>[] buffer;

    private Iterator<JetSqlRow> pos;

    private JetSqlRow currItem;
    private JetSqlRow pendingOutput;
    private final Watermark[] pendingWatermark;

    private final List<Watermark> delayedWatermark = new ArrayList<>();

    private final WatermarkCoalescer coalescer = WatermarkCoalescer.create(2);

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

        buffer = new PriorityQueue[2];
        buffer[0] = new PriorityQueue<>(comparingLong(leftTimestampExtractor));
        buffer[1] = new PriorityQueue<>(comparingLong(rightTimestampExtractor));

        pendingWatermark = new Watermark[2];
    }

    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (pendingOutput != null) {
            if (tryEmit(pendingOutput)) {
                pendingOutput = null;
            }
            return false;
        }

        //  having side input, traverse opposite buffer
        if (currItem == null) {
            currItem = (JetSqlRow) item;
            buffer[ordinal].offer(currItem);
            pos = buffer[1 - ordinal].iterator();
        }

        if (!pos.hasNext()) {
            pos = null;
            currItem = null;
            return true;
        }

        pendingOutput = ExpressionUtil.join(
                ordinal == 0 ? currItem : pos.next(),
                ordinal == 0 ? pos.next() : currItem,
                joinInfo.condition(),
                MOCK_EEC
        );

        if (pendingOutput != null && tryEmit(pendingOutput)) {
            pendingOutput = null;
        }
        return false;
    }

    /**
     * ...
     * 6. wm(d, 6)
     * 7. wm(o, 6)
     * 8. -> wm(d, 6)
     * 9. -> wm(o, -4)
     * ...
     * <p>
     * 6: We receive wm(d, 6). It means that we will receive no delivery items with d.time less than 6.
     * We cannot emit any watermark after this, as we still can receive order with any time (order's WM is -inf),
     * and when we receive order with t=-10, we must be able to join it with deliveries with t=-10..0.
     * This also means that we cannot remove any delivery rows from buffer.
     * <p>
     * 7: We receive wm(o, 6). Now we can emit wm(o, -4) and wm(d, 6).
     * The current value of watermarks for both keys is 6.
     * It means be able to process orders with t=-4, because order with t=-4 will join with delivery with t=6.
     * We still have them in the buffer and which we still can receive.
     * From this follows that we can also remove orders older than -4 and deliveries older than 6 from the buffers.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        if (pendingWatermark[ordinal] != null) {
            if (tryEmit(pendingWatermark[ordinal])) {
                pendingWatermark[ordinal] = null;
            }
            return false;
        }

        long ts = watermark.timestamp();
        long postponeTime = ts - postponeTimeMap[ordinal].get(watermark.key());
        if (postponeTime != ts) {
            watermark = new Watermark(postponeTime, watermark.key());
        }
        clearExpiredItemsInBuffer(
                buffer[ordinal],
                postponeTime,
                ordinal == 0 ? leftTimestampExtractor : rightTimestampExtractor);

        if (!tryEmit(watermark)) {
            pendingWatermark[ordinal] = watermark;
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
