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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.util.stream.Collectors.toList;

public class StreamToStreamJoinP extends AbstractProcessor {
    /**
     * <p>
     * JOIN condition should be transformed into such form:
     * <pre>
     *  l.time >= r.time - constant1
     *  r.time >= l.time - constant2
     * </pre>
     */
    private final JetJoinInfo joinInfo;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractor;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractor;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private final Tuple2<Integer, Integer> columnCount;

    private final Map<Byte, Map<Byte, Long>> wmState = new HashMap<>();

    private ExpressionEvalContext evalContext;
    private Iterator<JetSqlRow> pos;

    private JetSqlRow currItem;
    // NOTE: we are using LinkedList, because we are expecting :
    // (1) removals in the middle,
    // (2) traversing whole list without indexing.
    private final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};
    private final Queue<JetSqlRow> pendingOutput = new ArrayDeque<>();
    private final Queue<Watermark> pendingWatermarks = new ArrayDeque<>();

    public StreamToStreamJoinP(
            final JetJoinInfo joinInfo,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractor,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractor,
            final Map<Byte, Map<Byte, Long>> postponeTimeMap,
            final Tuple2<Integer, Integer> columnCount
    ) {
        this.joinInfo = joinInfo;
        this.leftTimeExtractor = leftTimeExtractor;
        this.rightTimeExtractor = rightTimeExtractor;
        this.postponeTimeMap = postponeTimeMap;
        this.columnCount = columnCount;

        for (byte key : postponeTimeMap.keySet()) {
            Map<Byte, Long> valueMap = postponeTimeMap.get(key);
            Map<Byte, Long> map = new HashMap<>();
            for (byte j : valueMap.keySet()) {
                map.put(j, Long.MIN_VALUE);
            }
            wmState.put(key, map);
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        this.evalContext = ExpressionEvalContext.from(context);
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        while (!pendingOutput.isEmpty()) {
            if (!tryEmit(pendingOutput.peek())) {
                return false;
            } else {
                pendingOutput.remove();
            }
        }

        //  having side input, traverse opposite buffer
        if (currItem == null) {
            currItem = (JetSqlRow) item;
            buffer[ordinal].add(currItem);
            pos = buffer[1 - ordinal].iterator();

            JetSqlRow joinedRow = null;
            // If current join type is LEFT/RIGHT and opposite buffer is empty,
            // we should to produce input row with null-filled opposite side.
            if (!pos.hasNext() && !joinInfo.isInner()) {
                if (ordinal == 1 && joinInfo.isLeftOuter()) {
                    // fill LEFT side with nulls
                    joinedRow = ExpressionUtil.join(
                            new JetSqlRow(currItem.getSerializationService(), new Object[columnCount.f0()]),
                            currItem,
                            joinInfo.condition(),
                            evalContext
                    );
                } else if (ordinal == 0 && joinInfo.isRightOuter()) {
                    // fill RIGHT side with nulls
                    joinedRow = ExpressionUtil.join(
                            currItem,
                            new JetSqlRow(currItem.getSerializationService(), new Object[columnCount.f1()]),
                            joinInfo.condition(),
                            evalContext
                    );
                }
            }

            if (joinedRow != null) {
                pendingOutput.offer(joinedRow);
            }
            return false;
        }

        if (!pos.hasNext()) {
            pos = null;
            currItem = null;
            return true;
        }

        JetSqlRow preparedOutput = ExpressionUtil.join(
                ordinal == 0 ? currItem : pos.next(),
                ordinal == 0 ? pos.next() : currItem,
                joinInfo.condition(),
                evalContext
        );

        if (preparedOutput != null && !tryEmit(preparedOutput)) {
            pendingOutput.offer(preparedOutput);
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        // if pending watermarks available - try to send them
        if (!pendingWatermarks.isEmpty()) {
            while (!pendingWatermarks.isEmpty()) {
                Watermark wm = pendingWatermarks.peek();
                if (!tryEmit(wm)) {
                    return false;
                } else {
                    pendingWatermarks.remove();
                }
            }
            return false;
        }

        // update wm state
        offer(watermark);

        // try to clear buffers if possible
        clearExpiredItemsInBuffer(ordinal, watermark);

        // We can't immediately emit current WM, as it would render items in left buffer late.
        // Instead, we can emit WM with the minimum available time for this WM key.
        long minimumItemTime = findMinimumBufferTime(ordinal, watermark);
        if (minimumItemTime != Long.MAX_VALUE) {
            Watermark wm = new Watermark(minimumItemTime, watermark.key());
            if (!tryEmit(wm)) {
                pendingWatermarks.offer(wm);
            }
        }

        return true;
    }

    private void offer(Watermark watermark) {
        byte key = watermark.key();
        final Map<Byte, Long> wmKeyMapping = postponeTimeMap.get(key);
        for (byte i : wmKeyMapping.keySet()) {
            Long curr = wmState.get(i).get(key);
            wmState.get(i).put(key, (curr != null && curr != Long.MIN_VALUE)
                    ? Math.min(watermark.timestamp() - wmKeyMapping.get(i), curr)
                    : watermark.timestamp() - wmKeyMapping.get(i));
        }
    }

    private long findMinimumBufferTime(int ordinal, Watermark watermark) {
        byte key = watermark.key();
        ToLongFunctionEx<JetSqlRow> extractor = ordinal == 0 ? leftTimeExtractor.get(key) : rightTimeExtractor.get(key);

        long min = Long.MAX_VALUE;
        for (JetSqlRow row : buffer[ordinal]) {
            min = Math.min(min, extractor.applyAsLong(row));
        }
        return min;
    }

    private long findMinimumGroupTime(byte group) {
        long min = Long.MAX_VALUE;
        for (byte i : wmState.get(group).keySet()) {
            min = Math.min(min, wmState.get(group).get(i));
        }
        return min;
    }

    private void clearExpiredItemsInBuffer(int ordinal, Watermark wm) {
        final ToLongFunctionEx<JetSqlRow> tsExtractor = ordinal == 0
                ? leftTimeExtractor.get(wm.key())
                : rightTimeExtractor.get(wm.key());

        long limit = findMinimumGroupTime(wm.key());
        if (limit == Long.MAX_VALUE || limit == Long.MIN_VALUE) {
            return;
        }

        buffer[ordinal] = buffer[ordinal]
                .stream()
                .filter(row -> tsExtractor.applyAsLong(row) >= limit)
                .collect(toList());
    }
}
