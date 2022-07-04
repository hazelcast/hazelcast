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

package com.hazelcast.jet.pipeline.test;

import java.io.Serializable;

/**
 * A function which takes a timestamp and a sequence number as the input.
 *
 * @param <R> the type of the result of the function
 *
 * @since Jet 3.2
 */
@FunctionalInterface
public interface GeneratorFunction<R> extends Serializable {

    /**
     * Applies the function to the given timestamp and sequence.
     *
     * @param timestamp the current timestamp
     * @param sequence the current sequence
     * @return the function result
     */
    R generate(long timestamp, long sequence) throws Exception;
}
