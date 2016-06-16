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

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;

import java.io.Serializable;

/**
 * Abstract JET processor for application's execution
 *
 * Should be implemented by user and will be used during container's execution
 *
 * There are two phases of execution:
 * <pre>
 *      1) Read phase. When Jet reads data from all input producers (previous vertices or taps) and
 *      can write or just collect somewhere input data
 *
 *      2) Finalization phase. When JET uses results of Read-phase (if it presents) and can write or not
 *      data to the output stream
 * </pre>
 *
 *
 * There is strict happens-before relation between execution of processor's execution.
 * So no needs to use thread-safe structures in case if task created separate processor
 * using corresponding factory
 *
 * @param <I> Type of the input object
 * @param <O> Type of the output object
 */
public interface ContainerProcessor<I, O> extends Serializable {
    /**
     * Will be invoked strictly  before first invocation of the corresponding task, strictly
     * from the executor thread
     *
     * @param processorContext context of processor
     */
    default void beforeProcessing(ProcessorContext processorContext) {

    }

    /**
     * Performs next iteration of execution
     *
     * @param inputStream      stream to be used for reading of data
     * @param outputStream     steam to be used for writing of data
     * @param sourceName       name of the source where data come from (Vertex or Tap)
     * @param processorContext context of processor
     * @return true if next chunk should be read, false if next iteration will be with the same inputStream
     * @throws Exception if any exception
     */
    default boolean process(ProducerInputStream<I> inputStream,
                    ConsumerOutputStream<O> outputStream,
                    String sourceName,
                    ProcessorContext processorContext
    ) throws Exception {
        return true;
    }

    /**
     * Will be invoked on finalization phase
     *
     * @param outputStream     outputSteam where data should be written
     * @param processorContext context of processor
     * @return true if finalization is finished, false if this method should be invoked again
     * @throws Exception if any exception
     */
    default boolean finalizeProcessor(ConsumerOutputStream<O> outputStream,
                              ProcessorContext processorContext) throws Exception {
        return true;
    }

    /**
     * Will be invoked strictly  after last invocation of the corresponding task, strictly
     * from the executor thread
     *
     * @param processorContext context of processor
     */
    default void afterProcessing(ProcessorContext processorContext) {

    }
}
