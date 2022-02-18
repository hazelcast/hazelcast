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

/**
 * Responsible for processing of an event of an {@link Accumulator}.
 *
 * Processing can vary according to the implementation.
 *
 * @param <T> type of element to process.
 * @see com.hazelcast.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor
 */
public interface AccumulatorProcessor<T> {

    /**
     * Processes event.
     *
     * @param event type of event.
     */
    void process(T event);
}
