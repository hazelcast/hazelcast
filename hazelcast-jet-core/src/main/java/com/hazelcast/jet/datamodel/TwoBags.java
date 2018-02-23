/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.datamodel;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.emptyList;

/**
 * A container of two bags (collections), each with its own element
 * type. Bags are identified by their index: 0 and 1. Useful as
 * a container of co-grouped data.
 *
 * @param <T0> type of items in bag-0
 * @param <T1> type of items in bag-1
 */
public final class TwoBags<T0, T1> {
    private final Collection<T0> bag0;
    private final Collection<T1> bag1;

    private TwoBags(@Nonnull Collection<T0> bag0, @Nonnull Collection<T1> bag1) {
        this.bag0 = new ArrayList<>(bag0);
        this.bag1 = new ArrayList<>(bag1);
    }

    /**
     * Returns a new, empty {@code TwoBags} container.
     */
    @Nonnull
    public static <E0, E1> TwoBags<E0, E1> twoBags() {
        return new TwoBags<>(emptyList(), emptyList());
    }

    /**
     * Returns a new {@code TwoBags} container populated with the supplied
     * collections. Doesn't retain the supplied collections, but
     * copies them into newly created ones.
     */
    @Nonnull
    public static <E0, E1> TwoBags<E0, E1> twoBags(@Nonnull Collection<E0> bag0, @Nonnull Collection<E1> bag1) {
        return new TwoBags<>(bag0, bag1);
    }

    /**
     * Retrieves the bag at index 0.
     */
    @Nonnull
    public Collection<T0> bag0() {
        return bag0;
    }

    /**
     * Retrieves the bag at index 1.
     */
    @Nonnull
    public Collection<T1> bag1() {
        return bag1;
    }

    /**
     * Combines this and the supplied container by merging all the supplied
     * container's data into this one. Leaves the supplied container unchanged.
     */
    public void combineWith(@Nonnull TwoBags<T0, T1> that) {
        bag0.addAll(that.bag0());
        bag1.addAll(that.bag1());
    }

    /**
     * Deducts the supplied container from this one by removing all the items
     * that the supplied one contains. Leaves the supplied container unchanged.
     */
    public void deduct(@Nonnull TwoBags<T0, T1> that) {
        bag0.removeAll(that.bag0());
        bag1.removeAll(that.bag1());
    }

    /**
     * Returns a safe copy of this container.
     */
    public TwoBags<T0, T1> finish() {
        return new TwoBags<>(new ArrayList<>(bag0), new ArrayList<>(bag1));
    }

    @Override
    public boolean equals(Object o) {
        TwoBags<?, ?> that;
        return this == o ||
                o instanceof TwoBags
                        && this.bag0.equals((that = (TwoBags<?, ?>) o).bag0)
                        && this.bag1.equals(that.bag1);
    }

    @Override
    public int hashCode() {
        int hc = bag0.hashCode();
        hc = 73 * hc + bag1.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "TwoBags{bag0=" + bag0 + ", bag1=" + bag1 + '}';
    }
}
