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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorHandler;

/**
 * Used to handle elements of an {@link Accumulator}.
 *
 * @param <T> the type of element in {@link Accumulator}.
 * @see PublisherAccumulatorHandler
 * @see com.hazelcast.map.impl.querycache.subscriber.SubscriberAccumulatorHandler
 */
public interface AccumulatorHandler<T> {

    /**
     * Handles element.
     *
     * @param element     the element to be processed.
     * @param lastElement {@code true} if this is the last element
     *                    got from the {@code Accumulator}, otherwise {@code false}.
     */
    void handle(T element, boolean lastElement);

    /**
     * Resets this handler to its initial state.
     */
    void reset();
}
