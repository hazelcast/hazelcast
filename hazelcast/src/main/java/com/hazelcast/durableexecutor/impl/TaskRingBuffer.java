/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.durableexecutor.StaleTaskIdException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

/**
 * A RingBuffer implementation to store tasks and results of those task
 * Only a single thread (partition-operation-thread) accesses the instance
 */
public class TaskRingBuffer {

    private Object[] ringItems;
    private int[] sequences;
    private boolean[] isTask;

    private int head = -1;
    private int callableCounter;

    public TaskRingBuffer() {
    }

    public TaskRingBuffer(int capacity) {
        this.ringItems = new Object[capacity];
        this.isTask = new boolean[capacity];
        this.sequences = new int[capacity];
    }

    /**
     * Adds the task to next available spot and returns the sequence corresponding to that spot.
     * throws exception if there is no available spot
     *
     * @param task The task
     * @return the sequence
     * @throws RejectedExecutionException if there is not available spot for the task
     */
    public int add(Callable task) {
        int index = findEmptySpot();
        callableCounter++;
        ringItems[index] = task;
        isTask[index] = true;
        sequences[index] = head;
        return head;
    }

    private int findEmptySpot() {
        if (callableCounter == ringItems.length) {
            throw new RejectedExecutionException("Capacity (" + ringItems.length + ") is reached!");
        }
        for (Object ringItem : ringItems) {
            head++;
            int index = toIndex(head);
            if (!isTask[index]) {
                return index;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Removes the task with the given sequence
     *
     * @param sequence the sequence
     */
    public void remove(int sequence) {
        int index = toIndex(sequence);
        ringItems[index] = null;
        isTask[index] = false;
        head--;
        callableCounter--;
    }

    /**
     * Puts the task for the given sequence
     *
     * @param sequence The sequence
     * @param task     The task
     */
    void putBackup(int sequence, Callable task) {
        head = Math.max(head, sequence);
        callableCounter++;
        int index = toIndex(sequence);
        ringItems[index] = task;
        isTask[index] = true;
        sequences[index] = sequence;
    }

    /**
     * Replaces the task with its response
     * If the sequence does not correspond to a task then the call is ignored
     *
     * @param sequence The sequence
     * @param response The response
     */
    void replaceTaskWithResult(int sequence, Object response) {
        int index = toIndex(sequence);
        // If sequence is not equal then it is disposed externally
        if (sequences[index] != sequence) {
            return;
        }
        ringItems[index] = response;
        isTask[index] = false;
        callableCounter--;
    }

    /**
     * Gets the response and disposes the sequence
     *
     * @param sequence The sequence
     * @return The response
     */
    Object retrieveAndDispose(int sequence) {
        int index = toIndex(sequence);
        checkSequence(index, sequence);
        try {
            return ringItems[index];
        } finally {
            ringItems[index] = null;
            isTask[index] = false;
            head--;
        }
    }

    /**
     * Disposes the sequence
     *
     * @param sequence The sequence
     */
    public void dispose(int sequence) {
        int index = toIndex(sequence);
        checkSequence(index, sequence);
        if (isTask[index]) {
            callableCounter--;
        }
        ringItems[index] = null;
        isTask[index] = false;
    }

    /**
     * Gets the response
     *
     * @param sequence The sequence
     * @return The response
     */
    public Object retrieve(int sequence) {
        int index = toIndex(sequence);
        checkSequence(index, sequence);
        return ringItems[index];
    }

    /**
     * Check if the sequence corresponds to a task
     *
     * @param sequence The sequence
     * @return <tt>true</tt> if the sequence corresponds to a task, <tt>false</tt> otherwise
     * @throws StaleTaskIdException if the solt overwritten
     */
    boolean isTask(int sequence) {
        int index = toIndex(sequence);
        checkSequence(index, sequence);
        return isTask[index];
    }

    private void checkSequence(int index, int sequence) {
        if (sequences[index] != sequence) {
            throw new StaleTaskIdException("The sequence has been overwritten");
        }
    }

    private int toIndex(int sequence) {
        return Math.abs(sequence % ringItems.length);
    }

    /**
     * Returns the number of currently running tasks in this ringbuffer.
     */
    public int getTaskSize() {
        return callableCounter;
    }

    public void write(ObjectDataOutput out) throws IOException {
        out.writeInt(head);
        out.writeInt(ringItems.length);
        for (int i = 0; i < ringItems.length; i++) {
            out.writeBoolean(isTask[i]);
            out.writeInt(sequences[i]);
            out.writeObject(ringItems[i]);
        }
    }

    public void read(ObjectDataInput in) throws IOException {
        head = in.readInt();
        int length = in.readInt();
        ringItems = new Object[length];
        isTask = new boolean[length];
        sequences = new int[length];
        for (int i = 0; i < length; i++) {
            isTask[i] = in.readBoolean();
            sequences[i] = in.readInt();
            ringItems[i] = in.readObject();
        }
    }

    public DurableIterator iterator() {
        return new DurableIterator();
    }

    public class DurableIterator implements Iterator {

        int index = -1;

        @Override
        public boolean hasNext() {
            return index + 1 < ringItems.length;
        }

        @Override
        public Object next() {
            if (++index == ringItems.length) {
                throw new NoSuchElementException();
            }
            return ringItems[index];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public int getSequence() {
            return sequences[index];
        }

        public boolean isTask() {
            return isTask[index];
        }
    }
}
