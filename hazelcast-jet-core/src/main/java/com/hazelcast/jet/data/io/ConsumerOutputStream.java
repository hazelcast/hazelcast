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

package com.hazelcast.jet.data.io;

/**
 * Represents abstract output-stream to consume data chunk of data-stream
 *
 * @param <T> type of consumed objects
 */
public interface ConsumerOutputStream<T> {
    /**
     * Consumes all objects from inputStream
     *
     * @param inputStream corresponding inputStream
     */
    void consumeStream(ProducerInputStream<T> inputStream);

    /**
     * Consumes chunk of objects with size actualSize
     *
     * @param chunk      chunk of objects
     * @param actualSize number of objects to consume
     */
    void consumeChunk(T[] chunk, int actualSize);

    /**
         * Method to consume an object
         *
         * @param object entity to consume
         * @return true if entry has been consumed
         * false if entry hasn't been consumed
         */
    boolean consume(T object);
}
