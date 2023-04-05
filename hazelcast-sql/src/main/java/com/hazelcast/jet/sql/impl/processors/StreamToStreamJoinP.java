/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.mapWithIndex;
import static com.hazelcast.internal.util.CollectionUtil.hasNonEmptyIntersection;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinBroadcastKeys.LAST_RECEIVED_WM_KEY;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinBroadcastKeys.WM_STATE_KEY;
import static java.util.function.Function.identity;

/**
 * See {@code docs/design/sql/15-stream-to-stream-join.md}.
 */
public class StreamToStreamJoinP extends AbstractProcessor {
    private static final long OBJECT_2_LONG_MAP_MIN_VALUE = Long.MIN_VALUE + 1;

    // package-visible for tests
    // tracks the current minimum event time for each watermark
    final Object2LongHashMap<Byte> wmState = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastReceivedWm = new Object2LongHashMap<>(Long.MIN_VALUE);
    final Object2LongHashMap<Byte> lastEmittedWm = new Object2LongHashMap<>(Long.MIN_VALUE);

    // package-visible for tests
    final StreamToStreamJoinBuffer[] buffer;

    private int[] processorPartitionKeys;
    private final JetJoinInfo joinInfo;
    private final int outerJoinSide;
    private final List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> leftTimeExtractors;
    private final List<Entry<Byte, ToLongFunctionEx<JetSqlRow>>> rightTimeExtractors;
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private final Tuple2<Integer, Integer> columnCounts;
    private long maxProcessorAccumulatedRecords;

    private ExpressionEvalContext evalContext;
    private ProcessingGuarantee processingGuarantee;
    private int processorIndex;

    private Iterator<JetSqlRow> iterator;
    private JetSqlRow currItem;

    private final Set<JetSqlRow> unusedEventsTracker = Collections.newSetFromMap(new IdentityHashMap<>());

    private final Queue<Object> pendingOutput = new ArrayDeque<>();
    private JetSqlRow emptyLeftRow;
    private JetSqlRow emptyRightRow;

    private Traverser<Entry<?, ?>> snapshotTraverser;

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
            wmState.put(wmKey, OBJECT_2_LONG_MAP_MIN_VALUE);
            lastEmittedWm.put(wmKey, OBJECT_2_LONG_MAP_MIN_VALUE);
            lastReceivedWm.put(wmKey, OBJECT_2_LONG_MAP_MIN_VALUE);
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

        this.buffer = createBuffers();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        evalContext = ExpressionEvalContext.from(context);
        SerializationService ss = evalContext.getSerializationService();
        emptyLeftRow = new JetSqlRow(ss, new Object[columnCounts.f0()]);
        emptyRightRow = new JetSqlRow(ss, new Object[columnCounts.f1()]);
        maxProcessorAccumulatedRecords = context.maxProcessorAccumulatedRecords();
        processingGuarantee = context.processingGuarantee();
        processorIndex = context.globalProcessorIndex();

        if (!joinInfo.isEquiJoin()) {
            JetServiceBackend jsb = getNodeEngine(context.hazelcastInstance()).getService(JetServiceBackend.SERVICE_NAME);
            int[] processorPartitionIds = context.processorPartitions();
            int[] partitionKeys = jsb.getSharedPartitionKeys();
            processorPartitionKeys = new int[processorPartitionIds.length];
            for (int i = 0; i < processorPartitionKeys.length; i++) {
                processorPartitionKeys[i] = partitionKeys[processorPartitionIds[i]];
            }
        }
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    @Override
    public boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert ordinal == 0 || ordinal == 1; // bad DAG
        if (!processPendingOutput()) {
            return false;
        }

        if (buffer[0].size() + buffer[1].size() >= maxProcessorAccumulatedRecords) {
            throw new AccumulationLimitExceededException();
        }

        // region item placement in buffer
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
            }
            // we'll emit joined rows from currItem and the buffered rows from the opposite side
            iterator = buffer[1 - ordinal].iterator();
            if (ordinal == outerJoinSide) {
                unusedEventsTracker.add(currItem);
            }
        }
        // endregion

        // region join procedure
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

        if (avoidBuffer && !joinInfo.isInner() && unusedEventsTracker.remove(currItem)) {
            JetSqlRow joinedRow = composeRowWithNulls(currItem, ordinal);
            if (joinedRow != null && !tryEmit(joinedRow)) {
                pendingOutput.add(joinedRow);
                return false;
            }
        }
        // endregion

        iterator = null;
        currItem = null;
        return true;
    }

    @Override
    public boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark) {
        if (!pendingOutput.isEmpty()) {
            return processPendingOutput();
        }

        Byte receivedWmKey = watermark.key();
        assert wmState.containsKey(receivedWmKey) : "unexpected watermark key: " + receivedWmKey;
        assert processingGuarantee != ProcessingGuarantee.EXACTLY_ONCE
                || lastReceivedWm.get(receivedWmKey) < watermark.timestamp() : "non-monotonic watermark: "
                + watermark.timestamp() + " when state is " + lastReceivedWm.get(receivedWmKey);

        lastReceivedWm.put(receivedWmKey, watermark.timestamp());

        // 5.1: update wm state
        boolean modified = applyToWmState(watermark);
        if (modified) {
            // TODO don't need to clean up particular edge, if nothing was changed for that edge
            clearExpiredItemsInBuffer(0);
            clearExpiredItemsInBuffer(1);
        }

        // Note: We can't immediately emit current WM, as it could render items in buffers late.
        for (Byte wmKey : wmState.keySet()) {
            long lastReceivedWatermark = lastReceivedWm.getValue(wmKey);
            long newWm = Math.min(wmState.get(wmKey), lastReceivedWatermark);
            if (newWm > lastEmittedWm.getValue(wmKey)) {
                pendingOutput.add(new Watermark(newWm, wmKey));
                lastEmittedWm.put(wmKey, newWm);
            }
        }

        return processPendingOutput();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            List<Entry<?, ?>> snapshotList = new ArrayList<>();
            Stream<Entry<?, ?>> leftBufferStream;
            Stream<Entry<?, ?>> rightBufferStream;

            for (Entry<Byte, Long> e : wmState.entrySet()) {
                Long timestamp = e.getValue();
                if (timestamp != OBJECT_2_LONG_MAP_MIN_VALUE) {
                    snapshotList.add(
                            entry(
                                    broadcastKey(WM_STATE_KEY),
                                    new WatermarkStateValue(e.getKey(), timestamp)
                            )
                    );
                }
            }

            for (Entry<?, Long> e : lastReceivedWm.entrySet()) {
                Long timestamp = e.getValue();
                if (timestamp != OBJECT_2_LONG_MAP_MIN_VALUE) {
                    snapshotList.add(
                            entry(
                                    broadcastKey(LAST_RECEIVED_WM_KEY),
                                    new WatermarkStateValue((Byte) e.getKey(), timestamp)
                            )
                    );
                }
            }

            if (joinInfo.isEquiJoin()) {
                leftBufferStream = buffer[0].content()
                        .stream()
                        .map(row -> entry(
                                ObjectArrayKey.project(row, joinInfo.leftEquiJoinIndices()),
                                new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 0)
                        ));

                rightBufferStream = buffer[1].content()
                        .stream()
                        .map(row -> entry(
                                ObjectArrayKey.project(row, joinInfo.rightEquiJoinIndices()),
                                new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 1)
                        ));

            } else {
                MutableInteger keyIndex = new MutableInteger();

                // Note: details are described in TDD: docs/design/sql/15-stream-to-stream-join.md
                if (joinInfo.isRightOuter()) {
                    if (processorIndex == 0) {
                        leftBufferStream = mapWithIndex(
                                buffer[0].content().stream(),
                                (row, index) -> entry(
                                        broadcastKey(index),
                                        new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 0)
                                ));
                    } else {
                        leftBufferStream = Stream.empty();
                    }
                    rightBufferStream = buffer[1].content()
                            .stream()
                            .map(row -> entry(
                                    processorPartitionKeys[cycle(keyIndex, processorPartitionKeys.length)],
                                    new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 1)));
                } else {
                    if (processorIndex == 0) {
                        rightBufferStream = mapWithIndex(
                                buffer[1].content().stream(),
                                (row, index) -> entry(
                                        broadcastKey(index),
                                        new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 1)
                                ));
                    } else {
                        rightBufferStream = Stream.empty();
                    }
                    leftBufferStream = buffer[0].content()
                            .stream()
                            .map(row -> entry(
                                    processorPartitionKeys[cycle(keyIndex, processorPartitionKeys.length)],
                                    new BufferSnapshotValue(row, unusedEventsTracker.contains(row), 0)));
                }
            }

            snapshotTraverser =
                    traverseStream(Stream.of(snapshotList.stream(), leftBufferStream, rightBufferStream)
                            .flatMap(identity()))
                            .onFirstNull(() -> snapshotTraverser = null);
        }

        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public boolean isCooperative() {
        return joinInfo.isCooperative();
    }

    /**
     * Increment the `value`. If equal to `max`, set to 0. Returns the new value.
     */
    private static int cycle(MutableInteger val, int max) {
        val.value++;
        if (val.value == max) {
            val.value = 0;
        }
        return val.value;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (value instanceof BufferSnapshotValue) {
            BufferSnapshotValue bsv = (BufferSnapshotValue) value;
            buffer[bsv.bufferOrdinal()].add(bsv.row());
            if (bsv.unused()) {
                unusedEventsTracker.add(bsv.row());
            }
            return;
        }

        if (key instanceof BroadcastKey) {
            BroadcastKey<?> broadcastKey = (BroadcastKey<?>) key;
            WatermarkStateValue wmValue = (WatermarkStateValue) value;
            // We pick minimal available watermark (both emitted/received) among all processors
            if (WM_STATE_KEY.equals(broadcastKey.key())) {
                Long stateWm = wmState.get(wmValue.key());
                boolean shouldUpdateReceived = stateWm <= OBJECT_2_LONG_MAP_MIN_VALUE || stateWm > wmValue.timestamp();
                if (shouldUpdateReceived) {
                    wmState.put(wmValue.key(), wmValue.timestamp());
                }
            } else if (LAST_RECEIVED_WM_KEY.equals(broadcastKey.key())) {
                Long stateWm = lastReceivedWm.get(wmValue.key());
                boolean shouldUpdateReceived = stateWm <= OBJECT_2_LONG_MAP_MIN_VALUE || stateWm > wmValue.timestamp();
                if (shouldUpdateReceived) {
                    lastReceivedWm.put(wmValue.key(), wmValue.timestamp());
                }
            } else {
                throw new JetException("Unexpected broadcast key: " + broadcastKey.key());
            }
        }
    }

    @Override
    public boolean closeIsCooperative() {
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

        for (int i = 0; i < extractors.size(); i++) {
            // TODO ignore time fields not in WM state
            limits[i] = wmState.getOrDefault(extractors.get(i).getKey(), Long.MIN_VALUE);
        }

        buffer[ordinal].clearExpiredItems(limits, row -> {
            if (outerJoinSide == ordinal && unusedEventsTracker.remove(row)) {
                // 5.4: If doing an outer join, emit events removed from the buffer,
                // with `null`s for the other side, if the event was never joined.
                JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
                if (joinedRow != null) {
                    pendingOutput.add(joinedRow);
                }
            }
        });
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

    private StreamToStreamJoinBuffer[] createBuffers() {
        return new StreamToStreamJoinBuffer[]{
                leftTimeExtractors.size() == 1
                        ? new StreamToStreamJoinHeapBuffer(leftTimeExtractors)
                        : new StreamToStreamJoinListBuffer(leftTimeExtractors),
                rightTimeExtractors.size() == 1
                        ? new StreamToStreamJoinHeapBuffer(rightTimeExtractors)
                        : new StreamToStreamJoinListBuffer(rightTimeExtractors)
        };
    }

    enum StreamToStreamJoinBroadcastKeys {
        WM_STATE_KEY,
        LAST_RECEIVED_WM_KEY
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

    private static final class BufferSnapshotValue implements DataSerializable {
        private JetSqlRow row;
        private boolean unused;
        private int bufferOrdinal;

        @SuppressWarnings("unused")
        BufferSnapshotValue() {
        }

        private BufferSnapshotValue(JetSqlRow row, boolean unused, int bufferOrdinal) {
            this.row = row;
            this.unused = unused;
            this.bufferOrdinal = bufferOrdinal;
        }

        public JetSqlRow row() {
            return row;
        }

        public int bufferOrdinal() {
            return bufferOrdinal;
        }

        public boolean unused() {
            return unused;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(bufferOrdinal);
            out.writeBoolean(unused);
            out.writeObject(row);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            bufferOrdinal = in.readInt();
            unused = in.readBoolean();
            row = in.readObject(JetSqlRow.class);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BufferSnapshotValue that = (BufferSnapshotValue) o;
            return unused == that.unused && bufferOrdinal == that.bufferOrdinal && row.equals(that.row);
        }

        @Override
        public int hashCode() {
            return Objects.hash(row, unused, bufferOrdinal);
        }

        @Override
        public String toString() {
            return "BufferSnapshotValue{" +
                    "row=" + row.get(0) +
                    ", unused=" + unused +
                    ", isLeftBuffer=" + bufferOrdinal +
                    '}';
        }
    }

    private static final class WatermarkStateValue implements DataSerializable {
        private Byte key;
        private Long timestamp;

        WatermarkStateValue() {
        }

        private WatermarkStateValue(Byte key, Long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        public Byte key() {
            return key;
        }

        public Long timestamp() {
            return timestamp;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeByte(key);
            out.writeLong(timestamp);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            key = in.readByte();
            timestamp = in.readLong();
        }

        @Override
        public String toString() {
            return "WatermarkValue{" +
                    "key=" + key +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
