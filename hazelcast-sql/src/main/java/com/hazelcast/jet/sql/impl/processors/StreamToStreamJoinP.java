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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
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

import static com.hazelcast.internal.util.CollectionUtil.hasNonEmptyIntersection;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinBroadcastKeys.LAST_EMITTED_KEYED_WM;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinBroadcastKeys.LAST_RECEIVED_KEYED_WM;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinBroadcastKeys.MIN_BUFFER_TIME;


/**
 * See {@code docs/design/sql/15-stream-to-stream-join.md}.
 */
public class StreamToStreamJoinP extends AbstractProcessor {
    private static final long WATERMARK_MAP_DEFAULT_VALUE = Long.MIN_VALUE + 1;
    private static final long MIN_BUFFER_TIME_DEFAULT_VALUE = Long.MAX_VALUE;

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

    // package-visible for tests
    final StreamToStreamJoinBuffer[] buffer;

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

    private Traverser<Entry> snapshotTraverser;

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
            wmState.put(wmKey, WATERMARK_MAP_DEFAULT_VALUE);
            lastEmittedWm.put(wmKey, WATERMARK_MAP_DEFAULT_VALUE);
            lastReceivedWm.put(wmKey, WATERMARK_MAP_DEFAULT_VALUE);
            minimumBufferTimes.put(wmKey, MIN_BUFFER_TIME_DEFAULT_VALUE);
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
        // endregion

        // region join procedure
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

    @Override
    public boolean saveToSnapshot() {
        // TODO (if possible): broadcast-distributed edges.

        if (snapshotTraverser == null) {
            // Note: in fact, first three collections contain from six to ten objects in total,
            //   it's not as much as you may think from first reading.
            int expectedCapacity = lastReceivedWm.size() + lastEmittedWm.size() + minimumBufferTimes.size()
                    + buffer[0].size() + buffer[1].size();

            List<Entry> snapshotList = new ArrayList<>(expectedCapacity);

            for (Entry<?, Long> e : lastEmittedWm.entrySet()) {
                Long timestamp = e.getValue();
                if (timestamp != WATERMARK_MAP_DEFAULT_VALUE) {
                    snapshotList.add(
                            entry(
                                    broadcastKey(LAST_EMITTED_KEYED_WM),
                                    WatermarkValue.wmValue((Byte) e.getKey(), timestamp)
                            )
                    );
                }
            }

            for (Entry<?, Long> e : lastReceivedWm.entrySet()) {
                Long timestamp = e.getValue();
                if (timestamp != WATERMARK_MAP_DEFAULT_VALUE) {
                    snapshotList.add(
                            entry(
                                    broadcastKey(LAST_RECEIVED_KEYED_WM),
                                    WatermarkValue.wmValue((Byte) e.getKey(), timestamp)
                            )
                    );
                }
            }

            for (Entry<?, Long> e : minimumBufferTimes.entrySet()) {
                Long timestamp = e.getValue();
                if (timestamp != MIN_BUFFER_TIME_DEFAULT_VALUE) {
                    snapshotList.add(
                            entry(
                                    broadcastKey(MIN_BUFFER_TIME),
                                    MinBufferTimeSnapshotValue.minBufferTimeValue((Byte) e.getKey(), timestamp)
                            )
                    );
                }
            }

            Stream<Entry> leftBufferStream = buffer[0].content()
                    .stream()
                    .map(row -> entry(
                            ObjectArrayKey.project(row, joinInfo.leftEquiJoinIndices()),
                            BufferSnapshotValue.bufferValue(row, 0)
                    ));

            Stream<Entry> rightBufferStream = buffer[1].content()
                    .stream()
                    .map(row -> entry(
                            ObjectArrayKey.project(row, joinInfo.rightEquiJoinIndices()),
                            BufferSnapshotValue.bufferValue(row, 1)
                    ));

            snapshotTraverser = Traversers.traverseStream(
                            Stream.concat(snapshotList.stream(),
                                    Stream.concat(leftBufferStream, rightBufferStream)))
                    .onFirstNull(() -> snapshotTraverser = null);
        }

        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            BroadcastKey broadcastKey = (BroadcastKey) key;

            if (value instanceof MinBufferTimeSnapshotValue) {
                MinBufferTimeSnapshotValue mbtsv = (MinBufferTimeSnapshotValue) value;
                // We pick minimal available time among all processors' time.
                if (minimumBufferTimes.get(mbtsv.wmKey()) > mbtsv.minBufferTime()) {
                    minimumBufferTimes.put(mbtsv.wmKey(), mbtsv.minBufferTime());
                }
                return;
            }

            WatermarkValue wmValue = (WatermarkValue) value;
            // We pick minimal available watermark (both emitted/received) among all processors
            if (LAST_EMITTED_KEYED_WM.equals(broadcastKey.key())) {
                Long lastEmittedWmTime = lastEmittedWm.get(wmValue.key());
                if (lastEmittedWmTime <= Long.MIN_VALUE + 1 ||
                        lastEmittedWmTime > wmValue.timestamp()) {
                    lastEmittedWm.put(wmValue.key(), wmValue.timestamp());
                }
            } else if (LAST_RECEIVED_KEYED_WM.equals(broadcastKey.key())) {
                Long lastReceivedWmTime = lastReceivedWm.get(wmValue.key());
                if (lastReceivedWmTime <= Long.MIN_VALUE + 1 ||
                        lastReceivedWmTime > wmValue.timestamp()) {
                    lastReceivedWm.put(wmValue.key(), wmValue.timestamp());
                }
            } else {
                throw new JetException("Unexpected broadcast key: " + broadcastKey.key());
            }

        } else {
            if (value instanceof BufferSnapshotValue) {
                BufferSnapshotValue bsv = (BufferSnapshotValue) value;
                buffer[bsv.bufferOrdinal()].add(bsv.row());
            } else {
                throw new AssertionError("Unreachable");
            }
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        // Note: buffer won't be cleaned up, because late and/or unmatched items
        //  won't be even added during normal processor work (line 211).

        // Process missing watermarks, see STSJPITest$test_equalTimes_singleWmKeyPerInput as a good example.
        for (Entry<Byte, Long> entry : lastReceivedWm.entrySet()) {
            if (entry.getValue() >= 0L) {
                Watermark wmToRestore = new Watermark(entry.getValue(), entry.getKey());
                applyToWmState(wmToRestore);
            }
        }

        for (Byte wmKey : wmState.keySet()) {
            long minimumBufferTime = minimumBufferTimes.get(wmKey);
            long lastReceivedWm = this.lastReceivedWm.getValue(wmKey);
            long newWmTime = Math.min(minimumBufferTime, lastReceivedWm);
            if (newWmTime > lastEmittedWm.getValue(wmKey)) {
                pendingOutput.add(new Watermark(newWmTime, wmKey));
                lastEmittedWm.put(wmKey, newWmTime);
            }
        }
        return true;
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

        long[] newMinimums = buffer[ordinal].clearExpiredItems(limits, row -> {
            if (outerJoinSide == ordinal && unusedEventsTracker.remove(row)) {
                // 5.4 : If doing an outer join, emit events removed from the buffer,
                // with `null`s for the other side, if the event was never joined.
                JetSqlRow joinedRow = composeRowWithNulls(row, ordinal);
                if (joinedRow != null) {
                    pendingOutput.add(joinedRow);
                }
            }
        });

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
        LAST_EMITTED_KEYED_WM,
        LAST_RECEIVED_KEYED_WM,
        MIN_BUFFER_TIME
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

        @Override
        public void init(@Nonnull Context context) throws Exception {
            if (!joinInfo.isEquiJoin() && context.processingGuarantee() != ProcessingGuarantee.NONE) {
                throw new UnsupportedOperationException(
                        "Non-equi-join fault-tolerant stream-to-stream JOIN is not supported");
            }
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
        private int bufferOrdinal;

        @SuppressWarnings("unused")
        BufferSnapshotValue() {
        }

        private BufferSnapshotValue(JetSqlRow row, int bufferOrdinal) {
            this.row = row;
            this.bufferOrdinal = bufferOrdinal;
        }

        public JetSqlRow row() {
            return row;
        }

        public int bufferOrdinal() {
            return bufferOrdinal;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(bufferOrdinal);
            out.writeObject(row);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            bufferOrdinal = in.readInt();
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
            return row.equals(that.row) && bufferOrdinal == that.bufferOrdinal;
        }

        @Override
        public int hashCode() {
            return Objects.hash(row, bufferOrdinal);
        }

        @Override
        public String toString() {
            return "BufferSnapshotValue{" +
                    "row=" + row.get(0) +
                    ", isLeftBuffer=" + bufferOrdinal +
                    '}';
        }

        public static BufferSnapshotValue bufferValue(@Nonnull JetSqlRow row, @Nonnull int bufferOrdinal) {
            return new BufferSnapshotValue(row, bufferOrdinal);
        }
    }

    private static final class WatermarkValue implements DataSerializable {
        private Byte key;
        private Long timestamp;

        WatermarkValue() {
        }

        private WatermarkValue(Byte key, Long timestamp) {
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

        public static WatermarkValue wmValue(@Nonnull Byte key, @Nonnull Long timestamp) {
            return new WatermarkValue(key, timestamp);
        }
    }

    private static final class MinBufferTimeSnapshotValue implements DataSerializable {
        private Byte wmKey;
        private Long minBufferTime;

        @SuppressWarnings("unused")
        MinBufferTimeSnapshotValue() {
        }

        private MinBufferTimeSnapshotValue(Byte wmKey, Long minBufferTime) {
            this.wmKey = wmKey;
            this.minBufferTime = minBufferTime;
        }

        public Byte wmKey() {
            return wmKey;
        }

        public Long minBufferTime() {
            return minBufferTime;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeByte(wmKey);
            out.writeLong(minBufferTime);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            wmKey = in.readByte();
            minBufferTime = in.readLong();
        }

        @Override
        public String toString() {
            return "MinBufferTimeSnapshotValue{" +
                    "wmKey=" + wmKey +
                    ", minBufferTime=" + minBufferTime +
                    '}';
        }

        public static MinBufferTimeSnapshotValue minBufferTimeValue(@Nonnull Byte wmKey, @Nonnull Long minBufferTime) {
            return new MinBufferTimeSnapshotValue(wmKey, minBufferTime);
        }
    }
}
