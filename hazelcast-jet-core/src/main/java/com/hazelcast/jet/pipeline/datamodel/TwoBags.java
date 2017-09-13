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

package com.hazelcast.jet.pipeline.datamodel;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A container of two bags (collections), each with its own element
 * type. Bags are identified by their index: 0 and 1. Useful as
 * a container of co-grouped data.
 *
 * @param <E0> type of items in bag-0
 * @param <E1> type of items in bag-1
 */
public class TwoBags<E0, E1> implements Serializable {
    private final Collection<E0> bag0;
    private final Collection<E1> bag1;

    /**
     * Constructs an empty {@code TwoBags} container.
     */
    public TwoBags() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    private TwoBags(Collection<E0> bag0, Collection<E1> bag1) {
        this.bag0 = bag0;
        this.bag1 = bag1;
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
}
