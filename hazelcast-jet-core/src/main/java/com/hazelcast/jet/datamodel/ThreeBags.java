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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A container of three bags (collections), each with its own element
 * type. Bags are identified by their index: 0, 1, and 2. Useful as
 * a container of co-grouped data.
 *
 * @param <E0> type of items in bag-0
 * @param <E1> type of items in bag-1
 * @param <E2> type of items in bag-2
 */
public class ThreeBags<E0, E1, E2> implements Serializable {
    private final Collection<E0> bag0;
    private final Collection<E1> bag1;
    private final Collection<E2> bag2;

    /**
     * Constructs an empty {@code ThreeBags} container.
     */
    public ThreeBags() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    private ThreeBags(@Nonnull List<E0> bag0, @Nonnull List<E1> bag1, @Nonnull List<E2> bag2) {
        this.bag0 = bag0;
        this.bag1 = bag1;
        this.bag2 = bag2;
    }

    /**
     * Retrieves the bag at index 0.
     */
    @Nonnull
    public Collection<E0> bag0() {
        return bag0;
    }

    /**
     * Retrieves the bag at index 1.
     */
    @Nonnull
    public Collection<E1> bag1() {
        return bag1;
    }

    /**
     * Retrieves the bag at index 2.
     */
    @Nonnull
    public Collection<E2> bag2() {
        return bag2;
    }

    /**
     * Combines this and the supplied container by merging all the supplied
     * container's data into this one. Leaves the supplied container unchanged.
     */
    public void combineWith(ThreeBags<E0, E1, E2> that) {
        bag0.addAll(that.bag0());
        bag1.addAll(that.bag1());
        bag2.addAll(that.bag2());
    }

    @Override
    public String toString() {
        return "ThreeBags{" + bag0 + ", " + bag1 + ", " + bag2 + '}';
    }
}
