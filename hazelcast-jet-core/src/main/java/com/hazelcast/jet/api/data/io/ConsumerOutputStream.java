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

package com.hazelcast.jet.api.data.io;

import com.hazelcast.jet.internal.api.actor.Consumer;

/**
 * Represents abstract output-stream to consume data chunk of data-stream;
 *
 * @param <T> - type of consumed objects;
 */
public interface ConsumerOutputStream<T> extends Consumer<T> {
    /**
     * Consumes all objects from inputStream;
     *
     * @param inputStream - corresponding inputStream;
     * @throws Exception if any exception
     */
    void consumeStream(ProducerInputStream<T> inputStream) throws Exception;

    /**
     * Consumes chunk of objects with size - actualSize;
     *
     * @param chunk      - chunk of objects;
     * @param actualSize - number of objects to consume;
     */
    void consumeChunk(T[] chunk, int actualSize);

    /**
     * Consumes one object;
     *
     * @param object - object to consume;
     * @return - always true for this type of consumer;
     * @throws Exception if any exception
     */
    boolean consume(T object) throws Exception;
}
