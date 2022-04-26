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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private final Tuple2<Integer, Integer> columnCount;

    private final Map<Byte, Map<Byte, Long>> wmState = new HashMap<>();

    private ExpressionEvalContext evalContext;
    private Iterator<JetSqlRow> iterator;

    private JetSqlRow currItem;
    // NOTE: we are using LinkedList, because we are expecting:
    // (1) removals in the middle,
    // (2) traversing whole list without indexing.
    private final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};
    private JetSqlRow pendingOutput;
    private Watermark pendingWatermark;

    public StreamToStreamJoinP(
            final JetJoinInfo joinInfo,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors,
            final Map<Byte, Map<Byte, Long>> postponeTimeMap,
            final Tuple2<Integer, Integer> columnCount
    ) {
        this.joinInfo = joinInfo;
        this.leftTimeExtractors = leftTimeExtractors;
        this.rightTimeExtractors = rightTimeExtractors;
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
        assert ordinal == 0 || ordinal == 1; // bad DAG

        if (pendingOutput != null && !tryEmit(pendingOutput)) {
            return false;
        }
        pendingOutput = null;

        //  having side input, traverse opposite buffer
        if (currItem == null) {
            currItem = (JetSqlRow) item;
            buffer[ordinal].add(currItem);
            iterator = buffer[1 - ordinal].iterator();

            JetSqlRow joinedRow = null;
            // If current join type is LEFT/RIGHT and opposite buffer is empty,
            // we should to produce input row with null-filled opposite side.
            if (!iterator.hasNext() && !joinInfo.isInner()) {
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
                assert pendingOutput == null;
                pendingOutput = joinedRow;
            }
            return false;
        }

        if (!iterator.hasNext()) {
            iterator = null;
            currItem = null;
            return true;
        }

        JetSqlRow preparedOutput = ExpressionUtil.join(
                ordinal == 0 ? currItem : iterator.next(),
                ordinal == 0 ? iterator.next() : currItem,
                joinInfo.condition(),
                evalContext
        );

        if (preparedOutput != null && !tryEmit(preparedOutput)) {
            assert pendingOutput == null;
            pendingOutput = preparedOutput;
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        // if pending watermarks available - try to send them
        if (pendingWatermark != null && !tryEmit(pendingWatermark)) {
            return false;
        }
        pendingWatermark = null;

        // update wm state
        offer(watermark);

        // try to clear buffers if possible
        clearExpiredItemsInBuffer(ordinal, watermark);

        // We can't immediately emit current WM, as it could render items in buffers late.
        // Instead, we can emit WM with the minimum available time for this WM key.
        long minimumItemTime = findMinimumBufferTime(ordinal, watermark);
        if (minimumItemTime != Long.MAX_VALUE) {
            Watermark wm = new Watermark(minimumItemTime, watermark.key());
            if (!tryEmit(wm)) {
                assert pendingWatermark == null;
                pendingWatermark = wm;
                return false;
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
        ToLongFunctionEx<JetSqlRow> extractor = ordinal == 0 ? leftTimeExtractors.get(key) : rightTimeExtractors.get(key);

        long min = Long.MAX_VALUE;
        for (JetSqlRow row : buffer[ordinal]) {
            min = Math.min(min, extractor.applyAsLong(row));
        }
        return min;
    }

    private long findMinimumGroupTime(byte group) {
        long min = Long.MAX_VALUE;
        for (long i : wmState.get(group).values()) {
            min = Math.min(min, i);
        }
        return min;
    }

    private void clearExpiredItemsInBuffer(int ordinal, Watermark wm) {
        final ToLongFunctionEx<JetSqlRow> tsExtractor = ordinal == 0
                ? leftTimeExtractors.get(wm.key())
                : rightTimeExtractors.get(wm.key());

        long limit = findMinimumGroupTime(wm.key());
        if (limit == Long.MAX_VALUE || limit == Long.MIN_VALUE) {
            return;
        }

        buffer[ordinal].removeIf(row -> tsExtractor.applyAsLong(row) < limit);
    }
}
