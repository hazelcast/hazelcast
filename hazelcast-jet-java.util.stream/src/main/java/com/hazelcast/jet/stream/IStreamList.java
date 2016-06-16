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

package com.hazelcast.jet.stream;

import com.hazelcast.core.IList;
import com.hazelcast.jet.stream.impl.ListDecorator;
import com.hazelcast.jet.stream.impl.StreamUtil;

/**
 * A decorator for {@link IList} for supporting distributed {@link java.util.stream.Stream}
 * implementation.
 *
 * @param <E> the type of elements in this list
 *
 * @see IList
 */
public interface IStreamList<E> extends IList<E> {

    /**
     * Returns a sequential {@code Stream} with this list as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     * @since 1.8
     */
    @Override
    DistributedStream<E> stream();

    /**
     * Returns a parallel {@code Stream} with this list as its source.
     *
     * @return a parallel {@code Stream} over the elements in this collection
     * @since 1.8
     */
    @Override
    DistributedStream<E> parallelStream();

    /**
     * Returns an {@link IList} with a {@link DistributedStream} support.
     *
     * @param list the Hazelcast list to decorate
     * @param <E> the type of elements in this list
     * @return Returns a {@link IList} with a {@link DistributedStream} support.
     */
    static <E> IStreamList<E> streamList(IList<E> list) {
        return new ListDecorator<>(list, StreamUtil.getHazelcastInstance(list));
    }
}
