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
import com.hazelcast.internal.serialization.SerializationService;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.lang.Long.MAX_VALUE;

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
    private final Set<JetSqlRow>[] unusedEventsTracker = new Set[]{new HashSet(), new HashSet()};
    private final Queue<JetSqlRow> pendingOutput = new ArrayDeque<>();
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
            iterator = buffer[1 - ordinal].iterator();
            if (!joinInfo.isInner()) {
                unusedEventsTracker[ordinal].add(currItem);
            }
        }

        if (!iterator.hasNext()) {
            iterator = null;
            currItem = null;
            return true;
        }

        JetSqlRow oppositeBufferItem = iterator.next();
        JetSqlRow preparedOutput = ExpressionUtil.join(
                ordinal == 0 ? currItem : oppositeBufferItem,
                ordinal == 0 ? oppositeBufferItem : currItem,
                joinInfo.condition(),
                evalContext
        );
        // it is used already once
        unusedEventsTracker[1 - ordinal].remove(oppositeBufferItem);

        if (preparedOutput != null && !tryEmit(preparedOutput)) {
            pendingOutput.offer(preparedOutput);
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
        applyToWmState(watermark);

        // try to clear buffers if possible
        clearExpiredItemsInBuffer(ordinal);

        // We can't immediately emit current WM, as it could render items in buffers late.
        // Instead, we can emit WM with the minimum available time for this WM key.
        byte wmKey = watermark.key();
        long minItemTime = findMinimumBufferTime(ordinal, wmKey);
        if (minItemTime != MAX_VALUE) {
            minItemTime = Math.min(findMinimumGroupTime(wmKey), minItemTime);
            if (minItemTime != Long.MIN_VALUE) {
                Watermark wm = new Watermark(minItemTime, wmKey);
                if (!tryEmit(wm)) {
                    assert pendingWatermark == null;
                    pendingWatermark = wm;
                    return false;
                }
            }
        }

        return true;
    }

    private void applyToWmState(Watermark watermark) {
        byte key = watermark.key();
        final Map<Byte, Long> wmKeyMapping = postponeTimeMap.get(key);

        for (byte i : wmKeyMapping.keySet()) {
            long newLimit = watermark.timestamp() - wmKeyMapping.get(i);
            wmState.get(key).put(i, newLimit);
        }
    }

    private long findMinimumBufferTime(int ordinal, byte key) {
        ToLongFunctionEx<JetSqlRow> extractor = ordinal == 0 ? leftTimeExtractors.get(key) : rightTimeExtractors.get(key);

        long min = MAX_VALUE;
        for (JetSqlRow row : buffer[ordinal]) {
            min = Math.min(min, extractor.applyAsLong(row));
        }
        return min;
    }

    private long findMinimumGroupTime(byte group) {
        long min = MAX_VALUE;
        for (long i : wmState.get(group).values()) {
            min = Math.min(min, i);
        }
        return min;
    }

    private void clearExpiredItemsInBuffer(int ordinal) {
        // remove items that are late according to all watermarks in them. Run after the wmState was changed.
        Map<Byte, ToLongFunctionEx<JetSqlRow>> currExtractors = ordinal == 0 ? leftTimeExtractors : rightTimeExtractors;
        ToLongFunctionEx<JetSqlRow>[] extractors = new ToLongFunctionEx[currExtractors.values().size()];
        long[] limits = new long[currExtractors.values().size()];

        int i = 0;
        for (Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>> entry : currExtractors.entrySet()) {
            extractors[i] = entry.getValue();
            limits[i] = findMinimumGroupTime(entry.getKey());
            ++i;
        }

        buffer[ordinal].removeIf(row -> {
            for (int idx = 0; idx < extractors.length; idx++) {
                if (extractors[idx].applyAsLong(row) >= limits[idx]) {
                    return false;
                }
            }
            System.err.println("To remove -> " + row + ", " + unusedEventsTracker[ordinal].contains(row));
            if (!joinInfo.isInner() && unusedEventsTracker[ordinal].contains(row)) {
                JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
                unusedEventsTracker[ordinal].remove(row);
                if (joinedRow != null) {
                    pendingOutput.offer(joinedRow);
                }
            }
            return true;
        });
    }

    // If current join type is LEFT/RIGHT and opposite buffer is empty,
    // we should to produce input row with null-filled opposite side.
    @SuppressWarnings("ConstantConditions")
    private JetSqlRow composeRowWithNulls(JetSqlRow row, int ordinal) {
        JetSqlRow joinedRow = null;
        SerializationService ss = row.getSerializationService();
        if (ordinal == 1 && joinInfo.isLeftOuter()) {
            // fill LEFT side with nulls
            joinedRow = ExpressionUtil.join(
                    new JetSqlRow(ss, new Object[columnCount.f0()]),
                    row,
                    joinInfo.condition(),
                    evalContext
            );
        } else if (ordinal == 0 && joinInfo.isRightOuter()) {
            // fill RIGHT side with nulls
            joinedRow = ExpressionUtil.join(
                    row,
                    new JetSqlRow(ss, new Object[columnCount.f1()]),
                    joinInfo.condition(),
                    evalContext
            );
        }
        return joinedRow;
    }
}
