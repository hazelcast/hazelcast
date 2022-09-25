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
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

/**
 * See {@code docs/design/sql/15-stream-to-stream-join.md}.
 */
public class StreamToStreamJoinP extends AbstractProcessor {
    // package-visible for tests
    final Object2LongHashMap<Byte> wmState = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastReceivedWm = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastEmittedWm = new Object2LongHashMap<>(Long.MIN_VALUE);

    /**
     * Maps wmKey to minimum buffer time for all items in the {@link #buffer}
     * for that WM. Must be updated when adding and removing anything to/from
     * the buffer.
     */
    final Object2LongHashMap<Byte> minimumBufferTimes = new Object2LongHashMap<>(Long.MIN_VALUE);

    // NOTE: we are using LinkedList, because we are expecting:
    //  (1) removals at any position,
    //  (2) no index-based access, only full traversal
    // package-visible for tests
    @SuppressWarnings("unchecked")
    final List<JetSqlRow>[] buffer = new List[]{new LinkedList<>(), new LinkedList<>()};

    private final JetJoinInfo joinInfo;
    private final int outerJoinSide;
    private final List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> leftTimeExtractors;
    private final List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> rightTimeExtractors;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private final Tuple2<Integer, Integer> columnCounts;
    private long maxProcessorAccumulatedRecords;

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
        this.leftTimeExtractors = new ArrayList<>(leftTimeExtractors.entrySet());
        this.rightTimeExtractors = new ArrayList<>(rightTimeExtractors.entrySet());
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
            lastReceivedWm.put(wmKey, Long.MIN_VALUE + 1);
            minimumBufferTimes.put(wmKey, Long.MAX_VALUE);
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
        maxProcessorAccumulatedRecords = context.maxProcessorAccumulatedRecords();
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert ordinal == 0 || ordinal == 1; // bad DAG
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        if (buffer[0].size() + buffer[1].size() >= maxProcessorAccumulatedRecords) {
            throw new AccumulationLimitExceededException();
        }

        boolean avoidBuffer = false;
        if (currItem == null) {
            // drop the event, if it's late according to any watermarked value it contains
            List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> extractors = timeExtractors(ordinal);
            long[] times = new long[extractors.size()];

            for (int i = 0; i < extractors.size(); i++) {
                long wmValue = lastReceivedWm.getValue(extractors.get(i).getKey());
                times[i] = extractors.get(i).getValue().applyAsLong((JetSqlRow) item);
                if (times[i] < wmValue) {
                    logLateEvent(getLogger(), extractors.get(i).getKey(), wmValue, item);
                    return true;
                }
            }

            // if the item is not late, but would already be removed from the buffer, don't add it to the buffer
            for (int i = 0; i < extractors.size(); i++) {
                long joinTimeLimit = wmState.get(extractors.get(i).getKey());
                long time = times[i];
                avoidBuffer |= time < joinTimeLimit;
            }

            currItem = (JetSqlRow) item;
            if (!avoidBuffer) {
                buffer[ordinal].add(currItem);
                // update the minimumBufferTimes when adding to buffer
                for (int i = 0; i < extractors.size(); i++) {
                    Byte wmKey = extractors.get(i).getKey();
                    if (times[i] < minimumBufferTimes.get(wmKey)) {
                        minimumBufferTimes.put(wmKey, times[i]);
                    }
                }
            }
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
                    joinInfo.isEquiJoin() ? joinInfo.condition() : joinInfo.nonEquiCondition(),
                    evalContext);

            if (preparedOutput == null) {
                continue;
            }

            if (ordinal == outerJoinSide) {
                // mark current item as used
                unusedEventsTracker.remove(currItem);
            } else if (ordinal == 1 - outerJoinSide) {
                // mark opposite-side item as used
                unusedEventsTracker.remove(oppositeBufferItem);
            }

            if (!tryEmit(preparedOutput)) {
                pendingOutput.add(preparedOutput);
                return false;
            }
        }

        iterator = null;

        if (avoidBuffer && !joinInfo.isInner() && unusedEventsTracker.remove(currItem)) {
            JetSqlRow joinedRow = composeRowWithNulls(currItem, ordinal);
            if (joinedRow != null && !tryEmit(joinedRow)) {
                pendingOutput.add(joinedRow);
                return false;
            }
        }
        currItem = null;

        return true;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        assert wmState.containsKey(watermark.key()) : "unexpected watermark key: " + watermark.key();
        assert lastReceivedWm.get(watermark.key()) < watermark.timestamp() : "non-monotonic watermark: " + watermark
                + " when state is " + lastReceivedWm.get(watermark.key());

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
            long minimumBufferTime = minimumBufferTimes.get(wmKey);
            long lastReceivedWm = this.lastReceivedWm.getValue(wmKey);
            long newWmTime = Math.min(minimumBufferTime, lastReceivedWm);
            if (newWmTime > lastEmittedWm.getValue(wmKey)) {
                pendingOutput.add(new Watermark(newWmTime, wmKey));
                lastEmittedWm.put(wmKey, newWmTime);
            }
        }

        return processPendingOutput();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
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

    private void clearExpiredItemsInBuffer(int ordinal) {
        List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> extractors = timeExtractors(ordinal);
        long[] limits = new long[extractors.size()];
        // when removing from the buffer, we'll also recalculate the minimum buffer times
        long[] newMinimums = new long[extractors.size()];
        Arrays.fill(newMinimums, Long.MAX_VALUE);

        for (int i = 0; i < extractors.size(); i++) {
            // TODO ignore time fields not in WM state
            limits[i] = wmState.getOrDefault(extractors.get(i).getKey(), Long.MIN_VALUE);
        }

        // Remove all expired events in left & right buffers
        // 5.4 : If doing an outer join, emit events removed from the buffer,
        // with `null`s for the other side, if the event was never joined.
        // TODO optimization: use PriorityQueues. At least in case of a single WM key.
        final Iterator<JetSqlRow> iterator = buffer[ordinal].iterator();
        long[] times = new long[extractors.size()];
        while (iterator.hasNext()) {
            JetSqlRow row = iterator.next();
            boolean remove = false;
            for (int idx = 0; idx < extractors.size(); idx++) {
                times[idx] = extractors.get(idx).getValue().applyAsLong(row);
                if (times[idx] < limits[idx]) {
                    remove = true;
                    if (unusedEventsTracker.remove(row) && outerJoinSide == ordinal) {
                        // 5.4 : If doing an outer join, emit events removed from the buffer,
                        // with `null`s for the other side, if the event was never joined.
                        JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
                        if (joinedRow != null) {
                            pendingOutput.add(joinedRow);
                        }
                    }
                }
            }

            if (remove) {
                iterator.remove();
            } else {
                for (int i = 0; i < times.length; i++) {
                    newMinimums[i] = Math.min(times[i], newMinimums[i]);
                }
            }
        }

        for (int i = 0; i < extractors.size(); i++) {
            minimumBufferTimes.put(extractors.get(i).getKey(), newMinimums[i]);
        }
    }

    private List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> timeExtractors(int ordinal) {
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

    public static final class StreamToStreamJoinProcessorSupplier implements ProcessorSupplier, DataSerializable {
        private JetJoinInfo joinInfo;
        private Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors;
        private Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors;
        private Map<Byte, Map<Byte, Long>> postponeTimeMap;
        private int leftInputColumnCount;
        private int rightInputColumnCount;

        @SuppressWarnings("unused") // for deserialization
        private StreamToStreamJoinProcessorSupplier() {
        }

        public StreamToStreamJoinProcessorSupplier(
                final JetJoinInfo joinInfo,
                final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors,
                final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors,
                final Map<Byte, Map<Byte, Long>> postponeTimeMap,
                final int leftInputColumnCount,
                final int rightInputColumnCount
        ) {
            this.joinInfo = joinInfo;
            this.leftTimeExtractors = leftTimeExtractors;
            this.rightTimeExtractors = rightTimeExtractors;
            this.postponeTimeMap = postponeTimeMap;
            this.leftInputColumnCount = leftInputColumnCount;
            this.rightInputColumnCount = rightInputColumnCount;

        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<StreamToStreamJoinP> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(
                        new StreamToStreamJoinP(
                                joinInfo,
                                leftTimeExtractors,
                                rightTimeExtractors,
                                postponeTimeMap,
                                Tuple2.tuple2(leftInputColumnCount, rightInputColumnCount)));
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            SerializationUtil.writeMap(leftTimeExtractors, out);
            SerializationUtil.writeMap(rightTimeExtractors, out);
            SerializationUtil.writeMap(postponeTimeMap, out);
            out.writeInt(leftInputColumnCount);
            out.writeInt(rightInputColumnCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            leftTimeExtractors = SerializationUtil.readMap(in);
            rightTimeExtractors = SerializationUtil.readMap(in);
            postponeTimeMap = SerializationUtil.readMap(in);
            leftInputColumnCount = in.readInt();
            rightInputColumnCount = in.readInt();
        }
    }
}
