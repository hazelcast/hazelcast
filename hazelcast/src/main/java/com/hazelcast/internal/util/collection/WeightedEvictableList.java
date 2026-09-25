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
 * Items are compared using {@link Object#equals(Object)}. Their equality
 * semantics should remain stable while they are stored in this list.
 * <p>
 * The list is not thread-safe.
 *
 * @param <T>
 */
public class WeightedEvictableList<T> {

    /*
     * This is a periodically reorganized priority list, not a list that is
     * always sorted by current weights. Reorganizing after every vote is
     * avoided; maxVotesBeforeReorganization controls how long items may
     * accumulate votes before the priority order is refreshed.
     *
     * When capacity is reached, existing items are sorted using their
     * historical votes and the lower-ranked half is removed. The survivors
     * retain their historical priority order ( they remain at the top of the list ),
     * but their weights are reset ( they become zero ) so
     * that previous winners cannot remain dominant forever and must prove
     * themselves again during the next voting period. A new item starts behind
     * those survivors and can move forward after accumulating votes.
     *
     * addOrVote scans the entire list, so items outside the leading positions
     * can continue receiving votes and eventually become preferred candidates.
     * This allows callers to use only the first few entries as a fast path
     * while retaining a larger pool of candidates for changing workloads.
     */

    /*
     * The list is maintained through two entry points.
     *
     * The first entry point is voteFor. A vote increments both the
     * item's weight and the number of votes cast since the last reorganization.
     * Once maxVotesBeforeReorganization votes have accumulated, the list is sorted
     * by weight and the vote counter is reset. This reorganization only refreshes
     * the order; it does not remove entries or reset their weights.
     *
     * The second entry point is addOrVote. It scans the complete list and,
     * if an equal item is found, delegates to the same voting path described above.
     * This allows entries outside the first few positions to continue receiving
     * votes and eventually move towards the front.
     *
     * If addOrVote does not find the item, the list is sorted before the new item
     * is added. If the list has reached maxSize, the lower-ranked half is removed.
     * The retained entries keep their order, but their weights are reset
     * to zero so that historically popular entries cannot remain dominant
     * forever. The new entry is then appended with a weight of one.
     *
     * Consequently, maxVotesBeforeReorganization controls how often the ranking
     * is refreshed, while maxSize controls when entries are removed and survivor
     * weights are reset. The list is not guaranteed to remain ordered by current
     * weight between reorganizations or immediately after a capacity-based
     * removal and insertion.
     */

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

        // null is a control signal meaning “organize without adding.”
        if (item == null) {
            return null;
        }

        if (list.size() == maxSize) {
            // drop the ones with the lower weights
            list.subList(retainedItemCount, list.size()).clear();

            // iterate over indexes so that no iterator is allocated
            for (int i = 0; i < list.size(); i++) {
                list.get(i).weight = 0;
            }
        }

        WeightedItem<T> weightedItem = new WeightedItem<>(item);
        weightedItem.weight = 1;
        list.add(weightedItem);

        return weightedItem;
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
