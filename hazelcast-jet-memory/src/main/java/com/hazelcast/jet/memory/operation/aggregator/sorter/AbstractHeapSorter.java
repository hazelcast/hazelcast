/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.memory.operation.aggregator.sorter;

import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.binarystorage.SortOrder;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.operation.aggregator.cursor.InputsCursor;
import com.hazelcast.jet.memory.util.JetIoUtil;

import java.util.Arrays;

import static com.hazelcast.jet.memory.operation.aggregator.sorter.AbstractHeapSorter.IndexDirection.LEFT;
import static com.hazelcast.jet.memory.operation.aggregator.sorter.AbstractHeapSorter.IndexDirection.RIGHT;

/**
 * Implements Merge-sort algorithm for two abstract input sources;
 * <p>
 * Implementation based on sorted Heap;
 * <p>
 * <pre>
 *     Input1, Input2, ... , InputN ->
 *         ____________
 *         |   Heap   |
 *         ------------
 *         |     1    |
 *         ------------
 *         |     3    |
 *         ------------
 *         |     4    |
 *         ------------
 *         |     5    |
 *         ------------
 *         |     2    |
 *         ------------
 *         |          |
 *         |  ......  |
 *         |          |
 *         ------------
 *         |     N    |
 *         ------------
 * </pre>
 * <p>
 * where indexes are sorted in each moment of time in accordance
 * with current input's record and comparator;
 *
 * @param <OUT> - type of output object;
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractHeapSorter<OUT> implements Sorter<InputsCursor, OUT> {
    private static final int HEAD_INDEX_IDX = 0;
    private static final int DUMMY_SOURCE_INPUT_INDEX = -1;

    protected OUT output;
    protected InputsCursor input;
    protected final boolean hasAccumulator;
    protected final Accumulator accumulator;
    protected final boolean hasAssociativeAccumulator;

    private final boolean isBigEndian;
    private final int chunkSize;
    private final SortOrder direction;
    private final MemoryBlock temporaryMemoryBlock;
    private final Comparator defaultComparator;
    private final ObjectHolder<Comparator> comparatorHolder;

    private int inputsCount;
    private int actualInputsCount;
    private int[] sortedHeap;

    private int leftUsed;
    private int leftActual;
    private int rightActual;

    private boolean[] inputsDone;
    private boolean[] leftEqualsFlag;
    private int pendingIndexId;

    enum IndexDirection { LEFT, RIGHT }

    protected AbstractHeapSorter(
            int chunkSize, SortOrder direction, ObjectHolder<Comparator> comparatorHolder,
            Comparator defaultComparator, Accumulator accumulator, MemoryBlock temporaryMemoryBlock,
            boolean isBigEndian
    ) {
        this.chunkSize = chunkSize;
        this.direction = direction;
        this.isBigEndian = isBigEndian;
        this.accumulator = accumulator;
        this.hasAccumulator = accumulator != null;
        this.hasAssociativeAccumulator = hasAccumulator && accumulator.isAssociative();
        this.comparatorHolder = comparatorHolder;
        this.temporaryMemoryBlock = temporaryMemoryBlock;
        this.defaultComparator = defaultComparator;
    }

    @Override
    public void resetTo(InputsCursor input, OUT output) {
        this.input = input;
        this.output = output;
        initSortedHeap(input);
        this.pendingIndexId = DUMMY_SOURCE_INPUT_INDEX;
        this.inputsCount = input.getInputsCount();
        this.actualInputsCount = this.inputsCount;
        this.leftActual = this.inputsCount;
        this.leftUsed = 0;
        this.rightActual = DUMMY_SOURCE_INPUT_INDEX;
    }

    private void initSortedHeap(InputsCursor input) {
        sortedHeap = reuseIfPossible(sortedHeap, input.getInputsCount());
        inputsDone = reuseIfPossible(inputsDone, input.getInputsCount());
        leftEqualsFlag = reuseIfPossible(leftEqualsFlag, input.getInputsCount());

        Arrays.fill(inputsDone, false);
        Arrays.fill(leftEqualsFlag, false);
        Arrays.fill(sortedHeap, DUMMY_SOURCE_INPUT_INDEX);
    }

    @Override
    public boolean sort() {
        if (checkPending()) {
            return false;
        }
        int writtenRecordCount = 0;
        while (writtenRecordCount < chunkSize) {
            if (refreshHeap()) {
                return true;
            }
            writtenRecordCount += applyAccumulator() ? calculate() : write();
        }
        return false;
    }

    protected abstract boolean applyNonAssociativeAccumulator();

    protected abstract void outputTuple(MemoryBlock memoryBlock, long recordAddress);

    protected abstract void outputSlot(InputsCursor iterator, int inputId);

    protected abstract void outputSegment(int sourceId, long recordsCount);

    private boolean checkPending() {
        return pendingIndexId != DUMMY_SOURCE_INPUT_INDEX && writeRecords() >= chunkSize;
    }

    private int calculate() {
        int inputId = sortedHeap[leftActual];
        leftActual++;
        MemoryBlock inputMemoryBlock = input.getMemoryBlock(inputId);
        long inputRecordAddress = input.recordAddress(inputId);
        long inputValueSize = input.valueSize(inputId);
        temporaryMemoryBlock.reset();
        temporaryMemoryBlock.copyFrom(inputMemoryBlock, inputRecordAddress,
                MemoryBlock.TOP_OFFSET, JetIoUtil.sizeOfTupleAt(inputRecordAddress, inputMemoryBlock.getAccessor()));
        long valueAddress = JetIoUtil.addrOfValueBlockAt(MemoryBlock.TOP_OFFSET, temporaryMemoryBlock.getAccessor());
        calculateInput(temporaryMemoryBlock, inputId, valueAddress, inputValueSize);
        calculateOverInputs(leftActual, temporaryMemoryBlock, valueAddress, inputValueSize);
        outputSlot(input, inputId);
        outputSegment(0, 1);
        outputTuple(temporaryMemoryBlock, MemoryBlock.TOP_OFFSET);
        return 1;
    }

    private void calculateOverInputs(
            int startFrom, MemoryBlock oldMemoryBlock, long oldValueAddress, long oldValueSize
    ) {
        int index = startFrom;
        while (index <= rightActual && leftEqualsFlag[index]) {
            int inputId = sortedHeap[index];
            leftActual = index + 1;
            if (inputsDone[inputId]) {
                continue;
            }
            if (inputId != DUMMY_SOURCE_INPUT_INDEX) {
                do {
                    MemoryBlock memoryBlock = input.getMemoryBlock(inputId);
                    long valueAddress = input.valueAddress(inputId);
                    long valueSize = input.valueSize(inputId);
                    accumulator.accept(oldMemoryBlock.getAccessor(), memoryBlock.getAccessor(),
                            oldValueAddress, oldValueSize, valueAddress, valueSize, isBigEndian);
                } while (input.nextRecord(inputId));
            }
            index++;
        }
    }

    private void calculateInput(
            MemoryBlock oldMemoryBlock, int inputId, long oldValueAddress, long oldValueSize
    ) {
        while (input.nextRecord(inputId)) {
            long valueAddress = input.valueAddress(inputId);
            long valueSize = input.valueSize(inputId);
            accumulator.accept(oldMemoryBlock.getAccessor(), input.getMemoryBlock(inputId).getAccessor(),
                    oldValueAddress, oldValueSize, valueAddress, valueSize, isBigEndian);
        }
    }

    private int write() {
        int index = leftActual;
        int writtenRecordsCount = 0;
        int inputId = sortedHeap[index];
        leftActual++;
        if (inputId == DUMMY_SOURCE_INPUT_INDEX || inputsDone[inputId]) {
            return writtenRecordsCount;
        }
        long recordsCount = calculateRecordsCount(index);
        outputSlot(input, inputId);
        outputSegment(0, recordsCount);
        outputTuple(input, inputId);
        writtenRecordsCount++;
        pendingIndexId = index;
        if (writtenRecordsCount >= chunkSize) {
            return writtenRecordsCount;
        }
        writtenRecordsCount += writeRecords();
        return writtenRecordsCount;
    }

    private int writeRecords() {
        int writtenRecordsCount = 0;
        while (true) {
            if (writtenRecordsCount >= chunkSize) {
                return writtenRecordsCount;
            }
            int inputId = sortedHeap[pendingIndexId];
            leftActual = pendingIndexId + 1;
            while (input.nextRecord(inputId)) {
                outputTuple(input, inputId);
                writtenRecordsCount++;
                if (writtenRecordsCount >= chunkSize) {
                    return writtenRecordsCount;
                }
            }
            if (pendingIndexId >= rightActual || !leftEqualsFlag[++pendingIndexId]) {
                break;
            }
            outputTuple(input, sortedHeap[pendingIndexId]);
            writtenRecordsCount++;
        }
        pendingIndexId = DUMMY_SOURCE_INPUT_INDEX;
        return writtenRecordsCount;
    }

    private long calculateRecordsCount(int index) {
        long recordsCount = 0;
        do {
            int inputId = sortedHeap[index];
            if (inputId != DUMMY_SOURCE_INPUT_INDEX) {
                recordsCount += input.recordsCount(inputId);
            }
            index++;
        } while (index < sortedHeap.length && leftEqualsFlag[index]);
        return recordsCount;
    }

    private boolean initHeap() {
        boolean done = true;
        Arrays.fill(sortedHeap, DUMMY_SOURCE_INPUT_INDEX);
        Arrays.fill(leftEqualsFlag, false);
        leftActual = inputsCount;
        leftUsed = HEAD_INDEX_IDX;
        rightActual = DUMMY_SOURCE_INPUT_INDEX;
        for (int inputId = HEAD_INDEX_IDX; inputId < inputsCount; inputId++) {
            if (inputsDone[inputId]) {
                continue;
            }
            int index = processInput(inputId);
            if (index != DUMMY_SOURCE_INPUT_INDEX) {
                done = false;
                refreshActual(index);
            }
        }
        return done;
    }

    private boolean refreshHeap() {
        if (leftActual >= inputsCount) {
            return initHeap();
        }
        int idx = leftUsed;
        int baseLeftActual = leftActual;
        while (idx < leftActual) {
            if (sortedHeap[idx] != DUMMY_SOURCE_INPUT_INDEX) {
                processInput(sortedHeap[idx]);
                if (baseLeftActual == leftActual) {
                    idx++;
                }
                unsetIndex(idx - 1);
                leftUsed = idx;
            } else {
                idx++;
            }
        }
        return actualInputsCount == 0;
    }

    private void unsetIndex(int idx) {
        if (idx >= HEAD_INDEX_IDX && idx < inputsCount) {
            sortedHeap[idx] = DUMMY_SOURCE_INPUT_INDEX;
            leftEqualsFlag[idx] = false;
            if (idx < inputsCount - 1) {
                leftEqualsFlag[idx + 1] = false;
            }
        }
    }

    private int processInput(int inputId) {
        if (inputsDone[inputId]) {
            return DUMMY_SOURCE_INPUT_INDEX;
        }
        if (input.nextSlot(inputId)) {
            input.nextSource(inputId);
            input.nextRecord(inputId);
            int index = lookupIndexInHeap(inputId);
            refreshActual(index);
            sortedHeap[index] = inputId;
            return index;
        } else {
            inputsDone[inputId] = true;
            this.actualInputsCount--;
            return DUMMY_SOURCE_INPUT_INDEX;
        }
    }

    private void refreshActual(int index) {
        leftActual = Math.min(leftActual, index);
        rightActual = Math.max(rightActual, index);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private int lookupIndexInHeap(int inputId) {
        if (leftActual == inputsCount) {
            return HEAD_INDEX_IDX;
        }
        if (actualInputsCount == 1) {
            return leftUsed;
        }
        MemoryBlock memoryBlock = input.getMemoryBlock(inputId);
        long keyAddress = input.keyAddress(inputId);
        long keySize = input.keySize(inputId);
        int leftBoundary = leftActual;
        int rightBoundary = rightActual;
        int heapIndexId = middle(leftBoundary, rightBoundary);
        while (true) {
            if (sortedHeap[heapIndexId] == DUMMY_SOURCE_INPUT_INDEX) {
                if (heapIndexId == 0) {
                    return 0;
                }
                rightBoundary = heapIndexId;
                heapIndexId = middle(leftBoundary, rightBoundary);
                continue;
            }
            int result = compare(memoryBlock, keyAddress, keySize, sortedHeap[heapIndexId]);
            if (result == 0) {
                return handleEquals(inputId, heapIndexId);
            }
            IndexDirection direction = getIndexDirection(result);
            if (direction == RIGHT) {
                if (heapIndexId == rightBoundary) {
                    return handleBoundary(rightBoundary, RIGHT);
                }
                leftBoundary = heapIndexId + 1;
                heapIndexId = nextRightIndex(rightBoundary, heapIndexId);
            } else {
                if (heapIndexId == leftBoundary) {
                    return handleBoundary(leftBoundary, direction);
                }
                rightBoundary = heapIndexId - 1;
                heapIndexId = nextLeftIndex(leftBoundary, heapIndexId);
            }
        }
    }

    private int handleBoundary(int boundary, IndexDirection direction) {
        if (direction == LEFT) {
            int resultIndex = boundary - 1;
            if (resultIndex > HEAD_INDEX_IDX && sortedHeap[resultIndex] == DUMMY_SOURCE_INPUT_INDEX) {
                return resultIndex;
            }
        } else {
            int resultIndex = boundary + 1;
            if (resultIndex < inputsCount && sortedHeap[resultIndex] == DUMMY_SOURCE_INPUT_INDEX) {
                return resultIndex;
            }
        }
        int realIndex = shiftHeap(boundary, direction);
        leftEqualsFlag[realIndex] = false;
        if (realIndex < inputsCount - 1) {
            leftEqualsFlag[realIndex + 1] = false;
        }
        return realIndex;
    }

    private static int nextLeftIndex(int leftBoundary, int heapIndexId) {
        heapIndexId = heapIndexId - 1 != leftBoundary ? middle(leftBoundary, heapIndexId) : leftBoundary;
        return heapIndexId;
    }

    private static int nextRightIndex(int rightBoundary, int heapIndexId) {
        heapIndexId = heapIndexId + 1 != rightBoundary ? middle(rightBoundary, heapIndexId) : rightBoundary;
        return heapIndexId;
    }

    private int handleEquals(int inputId, int heapIndexId) {
        int index = heapIndexId;
        int leftBoundary = findLeftBoundary(index);
        int rightBoundary = findRightBoundary(index);
        while (true) {
            if (inputId < sortedHeap[index]) {
                rightBoundary = index;
                if (!leftEqualsFlag[index]) {
                    int realIndex = shiftHeap(index, IndexDirection.LEFT);
                    setEqualsFlag(realIndex, false);
                    setEqualsFlag(realIndex + 1, true);
                    return realIndex;
                }
                index = middle(index, leftBoundary);
                continue;
            }
            if (inputId > sortedHeap[index]) {
                leftBoundary = index;
                if (index == inputsCount - 1) {
                    int realIndex = shiftHeap(index, IndexDirection.LEFT);
                    setEqualsFlag(realIndex, false);
                    setEqualsFlag(realIndex + 1, true);
                    return realIndex;
                }
                if (!leftEqualsFlag[index + 1]) {
                    int realIndex = shiftHeap(index, IndexDirection.RIGHT);
                    setEqualsFlag(realIndex, true);
                    setEqualsFlag(realIndex + 1, false);
                    return realIndex;
                }
                index = middle(index, rightBoundary);
            }
        }
    }

    private void setEqualsFlag(int index, boolean value) {
        if (index < inputsCount) {
            leftEqualsFlag[index] = value;
        }
    }

    private int findLeftBoundary(int heapIndexId) {
        int indexId = heapIndexId;
        while (indexId > 0 && leftEqualsFlag[indexId]) {
            indexId--;
        }
        return indexId;
    }

    private int findRightBoundary(int heapIndexId) {
        int indexId = heapIndexId;
        while (indexId < leftEqualsFlag.length - 1 && leftEqualsFlag[indexId + 1]) {
            indexId++;
        }
        return indexId;
    }

    private int compare(MemoryBlock memoryBlock, long keyAddress, long keyWrittenBytes, int heapIndexId) {
        long heapKeyAddress = input.keyAddress(heapIndexId);
        long heapKeyWrittenBytes = input.keySize(heapIndexId);
        MemoryBlock heapMemoryBlock = input.getMemoryBlock(heapIndexId);
        return comparatorHolder.
                                       get(defaultComparator).
                compare(memoryBlock.getAccessor(), heapMemoryBlock.getAccessor(),
                        keyAddress, keyWrittenBytes, heapKeyAddress, heapKeyWrittenBytes);
    }

    private int shiftHeap(int index, IndexDirection direction) {
        if (leftActual > HEAD_INDEX_IDX && index > HEAD_INDEX_IDX) {
            return moveToLeft(direction == LEFT ? index - 1 : index);
        } else if (index < inputsCount - 1) {
            return moveToRight(direction == LEFT ? index : index + 1);
        }
        throw new JetMemoryException("Fatal error inside heap sorter");
    }

    private int moveToLeft(int index) {
        shiftToLeft(sortedHeap, index);
        shiftToLeft(leftEqualsFlag, index);
        leftActual--;
        return index;
    }

    private int moveToRight(int index) {
        shiftToRight(sortedHeap, index);
        shiftToRight(leftEqualsFlag, index);
        rightActual++;
        return index;
    }

    private void shiftToRight(Object array, int index) {
        System.arraycopy(array, index, array, index + 1, inputsCount - index - 1);
    }

    private IndexDirection getIndexDirection(int result) {
        return direction == SortOrder.ASC
                ? result < 0 ? LEFT : RIGHT
                : result < 0 ? RIGHT : LEFT;
    }

    private boolean applyAccumulator() {
        return hasAssociativeAccumulator || hasAccumulator && applyNonAssociativeAccumulator();
    }

    private void outputTuple(InputsCursor iterator, int inputId) {
        outputTuple(iterator.getMemoryBlock(inputId), iterator.recordAddress(inputId));
    }

    private static void shiftToLeft(Object array, int index) {
        System.arraycopy(array, HEAD_INDEX_IDX + 1, array, HEAD_INDEX_IDX, index);
    }

    private static int middle(int index1, int index2) {
        return (index1 + index2) / 2;
    }

    private static int[] reuseIfPossible(int[] array, int cnt) {
        return array == null || array.length < cnt ? new int[cnt] : array;
    }

    private static boolean[] reuseIfPossible(boolean[] array, int cnt) {
        return array == null || array.length < cnt ? new boolean[cnt] : array;
    }
}
