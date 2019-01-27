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

/**
 * This list keeps the items as long as its size is less than maximum capacity.
 * Once the list size reaches {@code maxSize}, the half of the entries with
 * less weight are evicted.
 *
 * The list is not thread-safe.
 *
 * @param <T>
 */
public class WeightedEvictableList<T> {

    private List<WeightedItem<T>> list = new ArrayList<WeightedItem<T>>();

    private final int maxSize;
    private final int maxVotesBeforeReorganization;
    private int reorganizationCounter;

    private final Comparator<WeightedItem<T>> itemComparator = new Comparator<WeightedItem<T>>() {
        @Override
        public int compare(WeightedItem<T> o1, WeightedItem<T> o2) {
            return o2.weight - o1.weight;
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

    public List<WeightedItem<T>> getList() {
        return list;
    }

    /**
     * Casts a vote for given list node. This vote is added to the item's
     * weight.
     * @param weightedItem
     */
    public void voteFor(WeightedItem<T> weightedItem) {
        reorganizationCounter++;
        weightedItem.vote();
        if (reorganizationCounter == maxVotesBeforeReorganization) {
            reorganizationCounter = 0;
            organizeAndAdd(null);
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
        return organizeAndAdd(item);
    }

    WeightedItem<T> organizeAndAdd(T item) {
        Collections.sort(list, itemComparator);
        if (list.size() == maxSize) {
            if (item != null) {
                for (int i = list.size() - 1; i >= maxSize / 2; i--) {
                    list.remove(i);
                }
                for (WeightedItem<T> it : list) {
                    it.weight = 0;
                }
            }
        }
        WeightedItem<T> returnValue = null;
        if (item != null) {
            returnValue = new WeightedItem<T>(item);
            list.add(returnValue);
        }
        return returnValue;
    }

    /**
     * A node that contains an item and its weight
     * @param <T>
     */
    public static class WeightedItem<T> {

        final T item;
        int weight;

        WeightedItem(T item) {
            this.item = item;
            this.weight = 0;
        }

        WeightedItem(WeightedItem<T> other) {
            this.item = other.item;
            this.weight = other.weight;
        }

        private void vote() {
            weight++;
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
