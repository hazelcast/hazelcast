/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This list keeps the items as long as its size is less than maximum capacity.
 * Once the list size reaches {@code maxSize}, the half of the entries with
 * less weight are evicted.
 *
 * The list is thread-safe. However, its guarantees are rather weak because
 * of performance reasons. The new items are guaranteed to be added to the
 * list as long as there is not a concurrent add operation at the time.
 * When a number of threads concurrently calls {@link #add(Object)}, one
 * of the operations is guaranteed to succeed.
 *
 * {@link #voteFor(WeightedItem)} operations are dropped when an "add"
 * operation is active. Otherwise, they run concurrently. When a certain
 * number of {@link #voteFor(WeightedItem)} operations is called, this
 * list re-arranges its items so that the items are in ascending order
 * in terms of their weight. When the items are re-arranged, their weights
 * are reset. Between two re-arrangements, the list items keep insertion
 * order.
 *
 * This list is a copy-on-write list. Therefore, iterators retrieved
 * from snapshots are consistent.
 * @param <T>
 */
public class WeightedEvictableList<T> {

    private AtomicInteger reorganizationCounter = new AtomicInteger();
    private volatile boolean expansionInProgress;
    private Lock expansionLock = new ReentrantLock();
    private volatile List<WeightedItem<T>> list = Collections.EMPTY_LIST;

    private final int maxSize;
    private final int maxVotesBeforeReorganization;
    private final Comparator<WeightedItem<T>> itemComparator = new Comparator<WeightedItem<T>>() {
        @Override
        public int compare(WeightedItem<T> o1, WeightedItem<T> o2) {
            return o2.getWeightUnsafe() - o1.getWeightUnsafe();
        }
    };

    /**
     *
     * @param maxSize                       Maximum number of items this list
     *                                      can keep.
     * @param maxVotesBeforeReorganization  How many {@link #voteFor(WeightedItem)}
     *                                      operations are allowed, before items
     *                                      are re-ordered based on on their
     *                                      weights.
     */
    public WeightedEvictableList(int maxSize, int maxVotesBeforeReorganization) {
        this.maxSize = maxSize;
        this.maxVotesBeforeReorganization = maxVotesBeforeReorganization;
    }

    public List<WeightedItem<T>> getSnapshot() {
        return list;
    }

    /**
     * Casts a vote for given list node. This vote is added to the item's
     * weight.
     * @param weightedItem
     */
    public void voteFor(WeightedItem<T> weightedItem) {
        if (!expansionInProgress) {
            int reorganizationCount = reorganizationCounter.incrementAndGet();
            weightedItem.vote();
            if (reorganizationCount == maxVotesBeforeReorganization) {
                organize(null);
            }
        }
    }

    /**
     * Adds a new item to the list. If the list is full, the half of the
     * list is emptied. Removed half of the entries are the ones with
     * the least weight.
     * @param item
     * @return The node that can be used to vote for
     */
    public WeightedItem<T> add(T item) {
        return organize(item);
    }

    @SuppressWarnings("checkstyle:nestedifdepth")
    WeightedItem<T> organize(T item) {
        if (!expansionInProgress) {
            if (expansionLock.tryLock()) {
                expansionInProgress = true;
                List<WeightedItem<T>> originalList = list;
                List<WeightedItem<T>> copyList = new ArrayList<WeightedItem<T>>(originalList.size() + 1);
                for (int i = 0; i < originalList.size(); i++) {
                    if (!originalList.get(i).getItem().equals(item)) {
                        copyList.add(new WeightedItem<T>(originalList.get(i)));
                    }
                }
                Collections.sort(copyList, itemComparator);
                WeightedItem<T> returnValue = null;
                if (item != null) {
                    if (copyList.size() == maxSize) {
                        for (int i = copyList.size() - 1; i >= maxSize / 2; i--) {
                            copyList.remove(i);
                        }
                        for (int i = 0; i < maxSize / 2; i++) {
                            copyList.get(i).weight = 0;
                        }
                    }
                    returnValue = new WeightedItem<T>(item);
                    copyList.add(returnValue);
                }

                this.list = copyList;

                expansionInProgress = false;
                expansionLock.unlock();
                reorganizationCounter = new AtomicInteger();
                return returnValue;
            }
        }
        return null;
    }

    /**
     * A node that contains an item and its weight
     * @param <T>
     */
    public static class WeightedItem<T> {

        final T item;
        private volatile int weight;
        private final AtomicIntegerFieldUpdater<WeightedItem> weightUpdater =
                AtomicIntegerFieldUpdater.newUpdater(WeightedItem.class, "weight");

        WeightedItem(T item) {
            this.item = item;
            this.weight = 0;
        }

        WeightedItem(WeightedItem<T> other) {
            this.item = other.item;
            this.weight = other.weight;
        }

        private void vote() {
            weightUpdater.incrementAndGet(this);
        }

        int getWeightUnsafe() {
            return weight;
        }

        /**
         *
         * @return the stored item
         */
        public T getItem() {
            return item;
        }
    }
}
