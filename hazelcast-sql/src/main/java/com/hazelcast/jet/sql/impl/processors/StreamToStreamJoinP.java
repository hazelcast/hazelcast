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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import static java.lang.Long.MAX_VALUE;

@SuppressWarnings({"unchecked", "rawtypes"})
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
    private final Tuple2<Integer, Integer> columnCounts;

    private final Map<Byte, Map<Byte, Long>> wmState = new HashMap<>();
    private final Map<Byte, Long> lastEmittedWm = new HashMap<>();

    private ExpressionEvalContext evalContext;
    private Iterator<JetSqlRow> iterator;
    private JetSqlRow currItem;

    // NOTE: we are using LinkedList, because we are expecting:
    //  (1) removals in the middle,
    //  (2) traversing whole list without indexing.
    private final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};
    private final Set<JetSqlRow>[] unusedEventsTracker = new Set[]{new HashSet(), new HashSet()};

    private final Queue<Object> pendingOutput = new ArrayDeque<>();
    private JetSqlRow emptyLeftRow;
    private JetSqlRow emptyRightRow;

    public StreamToStreamJoinP(
            final JetJoinInfo joinInfo,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors,
            final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors,
            final Map<Byte, Map<Byte, Long>> postponeTimeMap,
            final Tuple2<Integer, Integer> columnCounts
    ) {
        this.joinInfo = joinInfo;
        this.leftTimeExtractors = leftTimeExtractors;
        this.rightTimeExtractors = rightTimeExtractors;
        this.postponeTimeMap = postponeTimeMap;
        this.columnCounts = columnCounts;

        for (Entry<Byte, Map<Byte, Long>> en : postponeTimeMap.entrySet()) {
            for (byte innerKey : en.getValue().keySet()) {
                wmState.computeIfAbsent(innerKey, x -> new HashMap<>()).put(en.getKey(), Long.MIN_VALUE);
                lastEmittedWm.put(innerKey, Long.MIN_VALUE);
            }
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        this.evalContext = ExpressionEvalContext.from(context);
        SerializationService ss = evalContext.getSerializationService();
        emptyLeftRow = new JetSqlRow(ss, new Object[columnCounts.f0()]);
        emptyRightRow = new JetSqlRow(ss, new Object[columnCounts.f1()]);
    }

    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert ordinal == 0 || ordinal == 1; // bad DAG
        if (!processPendingOutput()) {
            return false;
        }

        // having side input, traverse the opposite buffer
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
            pendingOutput.add(preparedOutput);
        }
        return false;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        // if watermark isn't present in watermark state, ignore it.
        if (!wmState.containsKey(watermark.key())) {
            return true;
        }

        // 5.1 : update wm state
        applyToWmState(watermark);

        // 5.2 & 5.3
        clearExpiredItemsInBuffer(0);
        clearExpiredItemsInBuffer(1);

        // Note: We can't immediately emit current WM, as it could render items in buffers late.

        // 5.5 : from the remaining elements in the buffer, compute
        // the minimum time value in each watermark timestamp column.
        for (Entry<Byte, Map<Byte, Long>> en : wmState.entrySet()) {
            long maxInputKeyGroupTime = findMaxInputKeyGroupTime(en.getValue().values());
            long minimumBufferTime = findMinimumBufferTime(ordinal, watermark.key());
            long newWmTime = Math.min(maxInputKeyGroupTime, minimumBufferTime);
            // 5.6 For each WM key, emit a new watermark
            // as the minimum of value computed in step 5
            // and of the last received value for that WM key.
            if (newWmTime > lastEmittedWm.get(en.getKey())) {
                pendingOutput.add(new Watermark(newWmTime, en.getKey()));
                lastEmittedWm.put(en.getKey(), newWmTime);
            }
        }

        return processPendingOutput();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean processPendingOutput() {
        while (!pendingOutput.isEmpty()) {
            if (!tryEmit(pendingOutput.peek())) {
                return false;
            } else {
                pendingOutput.remove();
            }
        }
        return true;
    }

    private void applyToWmState(Watermark watermark) {
        byte inputWmKey = watermark.key();
        Map<Byte, Long> wmKeyMapping = postponeTimeMap.get(inputWmKey);
        for (Map.Entry<Byte, Long> entry : wmKeyMapping.entrySet()) {
            Long newLimit = watermark.timestamp() - entry.getValue();
            assert wmState.get(entry.getKey()).get(inputWmKey) < newLimit : "old=" + wmState.get(entry.getKey()).get(inputWmKey) + ", new=" + newLimit;
            wmState.get(entry.getKey()).put(inputWmKey, newLimit);
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

    private long findMaxInputKeyGroupTime(Collection<Long> values) {
        long max = Long.MIN_VALUE;
        for (long value : values) {
            max = Math.max(max, value);
        }
        return max;
    }

    private void clearExpiredItemsInBuffer(int ordinal) {
        if (buffer[ordinal].isEmpty()) {
            return;
        }

        // 5.2 : compute new maximum for each output WM in the `wmState`.
        Map<Byte, ToLongFunctionEx<JetSqlRow>> currExtractors = ordinal == 0 ? leftTimeExtractors : rightTimeExtractors;
        ToLongFunctionEx<JetSqlRow>[] extractors = new ToLongFunctionEx[currExtractors.values().size()];
        long[] limits = new long[currExtractors.values().size()];

        int i = 0;
        for (Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>> entry : currExtractors.entrySet()) {
            extractors[i] = entry.getValue();
            limits[i] = findMaxInputKeyGroupTime(wmState.get(entry.getKey()).values());
            ++i;
        }

        // 5.3 Remove all expired events in left & right buffers
        buffer[ordinal].removeIf(row -> {
            for (int idx = 0; idx < extractors.length; idx++) {
                if (extractors[idx].applyAsLong(row) < limits[idx]) {
                    if (!joinInfo.isInner() && unusedEventsTracker[ordinal].contains(row)) {
                        // 5.4 : If doing an outer join, emit events removed from the buffer,
                        // with `null`s for the other side, if the event was never joined.
                        JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
                        unusedEventsTracker[ordinal].remove(row);
                        if (joinedRow != null) {
                            pendingOutput.add(joinedRow);
                        }
                    }
                    return true;
                }
            }
            return false;
        });
    }

    // If current join type is LEFT/RIGHT and a row don't have a matching row on the other side
    // we should to produce input row with null-filled opposite side.
    private JetSqlRow composeRowWithNulls(JetSqlRow row, int ordinal) {
        JetSqlRow joinedRow = null;
        if (ordinal == 1 && joinInfo.isLeftOuter()) {
            // fill LEFT side with nulls
            joinedRow = ExpressionUtil.join(
                    emptyLeftRow,
                    row,
                    joinInfo.condition(),
                    evalContext
            );
        } else if (ordinal == 0 && joinInfo.isRightOuter()) {
            // fill RIGHT side with nulls
            joinedRow = ExpressionUtil.join(
                    row,
                    emptyRightRow,
                    joinInfo.condition(),
                    evalContext
            );
        }
        return joinedRow;
    }
}
