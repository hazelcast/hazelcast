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

package com.hazelcast.jet.processor;

import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.data.io.OutputCollector;

import java.io.Serializable;

/**
 * A processor is the basic unit of execution in Jet. It consumes multiple inputs supplied by
 * {@link InputChunk}s and produces output to a {@link OutputCollector}.
 *
 * @param <I> Type of the input object
 * @param <O> Type of the output object
 */
public interface Processor<I, O> extends Serializable {

    /**
     * Invoked before the first call to process. Typically used to set up internal state
     * for the processor
     */
    default void before(TaskContext taskContext) {
    }

    /**
     * Process the next chunk of input data
     *
     * @return true if the current chunk is processed, false if not. The method will be called with the same
     * input chunk if false is returned.
     * @throws Exception when exception during processing
     */
    default boolean process(InputChunk<I> input, OutputCollector<O> output, String source)
            throws Exception {
        return true;
    }

    /**
     * Complete data processing. This method is called after all the input data is exhausted.
     *
     * @return true if data processing is complete. If false, the method will be called again later.
     * @throws Exception when exception during processing
     */
    default boolean complete(OutputCollector<O> output) throws Exception {
        return true;
    }

    /**
     * Invoked after the processor is completed. Typically used to cleanup state.
     */
    default void after() {
    }
}
