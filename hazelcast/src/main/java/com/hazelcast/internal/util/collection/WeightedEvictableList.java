/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This list keeps the items as long as its size is less than maximum
 * capacity. Once the list size reaches {@code maxSize}, the half of
 * the entries with less weight are evicted.
 * <p>
 * When a specified number of votes are cast the list is re-organized
 * to bring the items with the most votes in front. Also, every time
 * {@code maxSize} is reached, the list is reorganized.
 * <p>
 * The list is not thread-safe.
 *
 * @param <T>
 */
public class WeightedEvictableList<T> {

    private final List<WeightedItem<T>> list = new ArrayList<>();

    private final int maxSize;
    private final int retainedItemCount;
    private final int maxVotesBeforeReorganization;
    private int votesSinceLastReorganization;

    private final Comparator<WeightedItem<T>> itemComparator = (left, right) ->
            right.weight - left.weight;

    /**
     *
     * @param maxSize                       Maximum number of items this list
     *                                      can keep.
     * @param maxVotesBeforeReorganization  How many {@link #voteFor(WeightedItem)}
     *                                      operations are allowed, before items
     *                                      are re-ordered based on their
     *                                      weights.
     */
    public WeightedEvictableList(int maxSize, int maxVotesBeforeReorganization) {
        this.maxSize = maxSize;
        this.maxVotesBeforeReorganization = maxVotesBeforeReorganization;
        this.retainedItemCount = maxSize / 2;
    }

    public List<WeightedItem<T>> getList() {
        return list;
    }

    /**
     * Casts a vote for given list node. This vote is added to the item's
     * weight.
     */
    public void voteFor(WeightedItem<T> weightedItem) {
        votesSinceLastReorganization++;
        weightedItem.vote();
        if (votesSinceLastReorganization == maxVotesBeforeReorganization) {
            votesSinceLastReorganization = 0;
            organizeAndAdd(null);
        }
    }

    /**
     * Adds a new item to the list or votes for the given item if it
     * already exists. If the {@link #maxSize} is reached, half of the
     * list is removed.
     * <p>
     * When half of the list is removed, the weights of all the items
     * are reset. The newly added item gets a vote if applicable.
     *
     * @return The node that can be used to vote for
     */
    public WeightedItem<T> addOrVote(T item) {
        // iterate over indexes so that no iterator is allocated
        for (int i = 0; i < list.size(); i++) {
            WeightedItem<T> weightedItem = list.get(i);
            if (weightedItem.item.equals(item)) {
                voteFor(weightedItem);
                return weightedItem;
            }
        }
        return organizeAndAdd(item);
    }

    public WeightedItem<T> getWeightedItem(int index) {
        return list.get(index);
    }

    public int size() {
        return list.size();
    }

    WeightedItem<T> organizeAndAdd(T item) {
        list.sort(itemComparator);
        if (list.size() == maxSize) {
            if (item != null) {
                if (list.size() > retainedItemCount) {
                    list.subList(retainedItemCount, list.size()).clear();
                }
                for (WeightedItem<T> it : list) {
                    it.weight = 0;
                }
            }
        }
        WeightedItem<T> returnValue = null;
        if (item != null) {
            returnValue = new WeightedItem<>(item);
            returnValue.weight = 1;
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
