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
import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.internal.util.CollectionUtil.hasNonEmptyIntersection;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static java.lang.Long.MAX_VALUE;

public class StreamToStreamJoinP extends AbstractProcessor {
    // package-visible for tests
    final Object2LongHashMap<Byte> wmState = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastReceivedWm = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastEmittedWm = new Object2LongHashMap<>(Long.MIN_VALUE);

    // NOTE: we are using LinkedList, because we are expecting:
    //  (1) removals at any position,
    //  (2) no index-based access, only full traversal
    // package-visible for tests
    @SuppressWarnings("unchecked") final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};

    private final JetJoinInfo joinInfo;
    private final int outerJoinSide;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors;
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private final Tuple2<Integer, Integer> columnCounts;

    private ExpressionEvalContext evalContext;
    private Iterator<JetSqlRow> iterator;
    private JetSqlRow currItem;

    private final Set<JetSqlRow> unusedEventsTracker = Collections.newSetFromMap(new IdentityHashMap<>());

    private final Queue<Object> pendingOutput = new ArrayDeque<>();
    private JetSqlRow emptyLeftRow;
    private JetSqlRow emptyRightRow;

    @SuppressWarnings("checkstyle:ExecutableStatementCount")
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

        switch (joinInfo.getJoinType()) {
            case INNER:
                outerJoinSide = -1;
                break;
            case LEFT:
                outerJoinSide = 0;
                break;
            case RIGHT:
                outerJoinSide = 1;
                break;
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinInfo.getJoinType());
        }

        for (Byte wmKey : postponeTimeMap.keySet()) {
            // using MIN_VALUE + 1 because Object2LongHashMap uses MIN_VALUE as a missing value, and it cannot be used as a value
            wmState.put(wmKey, Long.MIN_VALUE + 1);
            lastEmittedWm.put(wmKey, Long.MIN_VALUE + 1);
        }

        // no key must be on both sides
        if (hasNonEmptyIntersection(leftTimeExtractors.keySet(), rightTimeExtractors.keySet())) {
            throw new IllegalArgumentException("Some watermark key is found on both inputs. Left="
                    + leftTimeExtractors.keySet() + ", right=" + rightTimeExtractors.keySet());
        }

        // postponeTimeMap must contain at least one bound for a key on left, involving a key on right, and vice versa
        boolean[] found = new boolean[2];
        for (Entry<Byte, Map<Byte, Long>> outerEntry : postponeTimeMap.entrySet()) {
            for (Byte innerKey : outerEntry.getValue().keySet()) {
                int innerOrdinal = leftTimeExtractors.containsKey(innerKey) ? 0 : 1;
                int outerOrdinal = leftTimeExtractors.containsKey(outerEntry.getKey()) ? 0 : 1;
                // innerOrdinal == outerOrdinal if the time bound is between timestamps on the same input, we ignore those
                if (innerOrdinal != outerOrdinal) {
                    found[innerOrdinal] = true;
                }
            }
        }
        if (!found[0] || !found[1]) {
            throw new IllegalArgumentException("Not enough time bounds in postponeTimeMap");
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        this.evalContext = ExpressionEvalContext.from(context);
        SerializationService ss = evalContext.getSerializationService();
        emptyLeftRow = new JetSqlRow(ss, new Object[columnCounts.f0()]);
        emptyRightRow = new JetSqlRow(ss, new Object[columnCounts.f1()]);
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert ordinal == 0 || ordinal == 1; // bad DAG
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        if (currItem == null) {
            // drop the event, if it's late according to any watermarked value it contains
            for (Entry<Byte, ToLongFunctionEx<JetSqlRow>> en : timeExtractors(ordinal).entrySet()) {
                long wmValue = lastReceivedWm.getValue(en.getKey());
                long time = en.getValue().applyAsLong((JetSqlRow) item);
                if (time < wmValue) {
                    logLateEvent(getLogger(), en.getKey(), wmValue, item);
                    return true;
                }
            }

            // if the item is not late, but would already be removed from the buffer, don't add it to the buffer
            for (Entry<Byte, ToLongFunctionEx<JetSqlRow>> en : timeExtractors(ordinal).entrySet()) {
                long joinTimeLimit = wmState.get(en.getKey());
                long time = en.getValue().applyAsLong((JetSqlRow) item);
                if (time < joinTimeLimit) {
                    if (!joinInfo.isInner()) {
                        JetSqlRow joinedRow = composeRowWithNulls((JetSqlRow) item, ordinal);
                        if (joinedRow != null && !tryEmit(joinedRow)) {
                            pendingOutput.add(joinedRow);
                            return false;
                        }
                    }
                    return true;
                }
            }

            currItem = (JetSqlRow) item;
            buffer[ordinal].add(currItem);
            // we'll emit joined rows from currItem and the buffered rows from the opposite side
            iterator = buffer[1 - ordinal].iterator();
            if (ordinal == outerJoinSide) {
                unusedEventsTracker.add(currItem);
            }
        }

        while (iterator.hasNext()) {
            JetSqlRow oppositeBufferItem = iterator.next();
            JetSqlRow preparedOutput = ExpressionUtil.join(
                    ordinal == 0 ? currItem : oppositeBufferItem,
                    ordinal == 0 ? oppositeBufferItem : currItem,
                    joinInfo.condition(),
                    evalContext);

            if (preparedOutput == null) {
                continue;
            }
            if (ordinal == 1 - outerJoinSide) {
                // mark opposite-side item as used
                unusedEventsTracker.remove(oppositeBufferItem);
            }
            if (!tryEmit(preparedOutput)) {
                pendingOutput.add(preparedOutput);
                return false;
            }
        }

        iterator = null;
        currItem = null;
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        assert wmState.containsKey(watermark.key()) : "unexpected watermark key: " + watermark.key();
        assert wmState.get(watermark.key()) < watermark.timestamp() : "non-monotonic watermark: " + watermark;

        lastReceivedWm.put((Byte) watermark.key(), watermark.timestamp());

        // 5.1 : update wm state
        boolean modified = applyToWmState(watermark);
        if (modified) {
            // TODO don't need to clean up particular edge, if nothing was changed for that edge
            clearExpiredItemsInBuffer(0);
            clearExpiredItemsInBuffer(1);
        }

        // Note: We can't immediately emit current WM, as it could render items in buffers late.

        for (Byte wmKey : wmState.keySet()) {
            long minimumBufferTime = findMinimumBufferTime(wmKey);
            long lastReceivedWm = this.lastReceivedWm.getValue(wmKey);
            long newWmTime = Math.min(minimumBufferTime, lastReceivedWm);
            if (newWmTime > lastEmittedWm.getValue(wmKey)) {
                pendingOutput.add(new Watermark(newWmTime, wmKey));
                lastEmittedWm.put(wmKey, newWmTime);
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

    /**
     * @return Returns true, if the state was modified
     */
    private boolean applyToWmState(Watermark watermark) {
        boolean modified = false;
        Byte inputWmKey = watermark.key();
        Map<Byte, Long> wmKeyMapping = postponeTimeMap.get(inputWmKey);
        for (Map.Entry<Byte, Long> entry : wmKeyMapping.entrySet()) {
            Long newLimit = watermark.timestamp() - entry.getValue();
            Long oldLimit = wmState.get(entry.getKey());
            if (newLimit > oldLimit) {
                wmState.put(entry.getKey(), newLimit);
                modified = true;
            }
        }
        return modified;
    }

    private long findMinimumBufferTime(byte key) {
        // TODO optimization: use PriorityQueues. At least in case of a single WM key.
        ToLongFunctionEx<JetSqlRow> extractor = leftTimeExtractors.get(key);
        int ordinal = 0;
        if (extractor == null) {
            extractor = rightTimeExtractors.get(key);
            ordinal = 1;
        } else {
            assert !rightTimeExtractors.containsKey(key) : "extractor for the same key on both inputs";
        }

        long min = MAX_VALUE;
        for (JetSqlRow row : buffer[ordinal]) {
            min = Math.min(min, extractor.applyAsLong(row));
        }
        return min;
    }

    private void clearExpiredItemsInBuffer(int ordinal) {
        if (buffer[ordinal].isEmpty()) {
            return;
        }

        // Compute new maximum for each output WM in the `wmState`.
        Map<Byte, ToLongFunctionEx<JetSqlRow>> currExtractors = timeExtractors(ordinal);
        @SuppressWarnings("unchecked")
        ToLongFunctionEx<JetSqlRow>[] extractors = new ToLongFunctionEx[currExtractors.values().size()];
        long[] limits = new long[currExtractors.values().size()];

        int i = 0;
        for (Map.Entry<Byte, ToLongFunctionEx<JetSqlRow>> entry : currExtractors.entrySet()) {
            extractors[i] = entry.getValue();
            // TODO ignore time fields not in WM state
            limits[i] = wmState.getOrDefault(entry.getKey(), Long.MIN_VALUE);
            ++i;
        }

        // Remove all expired events in left & right buffers
        buffer[ordinal].removeIf(row -> {
            for (int idx = 0; idx < extractors.length; idx++) {
                if (extractors[idx].applyAsLong(row) < limits[idx]) {
                    if (outerJoinSide == ordinal && unusedEventsTracker.remove(row)) {
                        // 5.4 : If doing an outer join, emit events removed from the buffer,
                        // with `null`s for the other side, if the event was never joined.
                        JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
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

    private Map<Byte, ToLongFunctionEx<JetSqlRow>> timeExtractors(int ordinal) {
        return ordinal == 0 ? leftTimeExtractors : rightTimeExtractors;
    }

    // If current join type is LEFT/RIGHT and a row don't have a matching row on the other side
    // we should to produce input row with null-filled opposite side.
    private JetSqlRow composeRowWithNulls(JetSqlRow row, int ordinal) {
        JetSqlRow joinedRow = null;
        if (ordinal == 1 && joinInfo.isRightOuter()) {
            // fill LEFT side with nulls
            joinedRow = ExpressionUtil.join(
                    emptyLeftRow,
                    row,
                    ConstantExpression.TRUE,
                    evalContext
            );
        } else if (ordinal == 0 && joinInfo.isLeftOuter()) {
            // fill RIGHT side with nulls
            joinedRow = ExpressionUtil.join(
                    row,
                    emptyRightRow,
                    ConstantExpression.TRUE,
                    evalContext
            );
        }
        return joinedRow;
    }
}
