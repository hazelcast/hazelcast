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

package com.hazelcast.jet.spi.processor.tuple;

import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;

/**
 * Represents abstract container which can work with JET tuples;
 *
 * @param <KeyInput>    - type of input tuple key part;
 * @param <ValueInput>  - type of input tuple value part;
 * @param <KeyOutPut>   - type of output tuple  key part;
 * @param <ValueOutPut> - type of output tuple  value part;
 */
public interface TupleContainerProcessor<KeyInput, ValueInput, KeyOutPut, ValueOutPut>
        extends ContainerProcessor<Tuple<KeyInput, ValueInput>, Tuple<KeyOutPut, ValueOutPut>> {
    /**
     * Performs next iteration of execution;
     *
     * @param inputStream      - stream to be used for reading of data;
     * @param outputStream     - steam to be used for writing of data;
     * @param sourceName       - name of the source where data come from (Vertex or Tap);
     * @param processorContext - context of processor;
     * @return - true - if next chunk should be read, false if next iteration will be with the same inputStream;
     * @throws Exception
     */
    boolean process(ProducerInputStream<Tuple<KeyInput, ValueInput>> inputStream,
                    ConsumerOutputStream<Tuple<KeyOutPut, ValueOutPut>> outputStream,
                    String sourceName,
                    ProcessorContext processorContext
    ) throws Exception;

    /**
     * Will be invoked on finalization phase;
     *
     * @param outputStream     - outputSteam where data should be written;
     * @param processorContext - context of processor;
     * @return - true if finalization is finished, false if this method should be invoked again;
     * @throws Exception
     */
    boolean finalizeProcessor(ConsumerOutputStream<Tuple<KeyOutPut, ValueOutPut>> outputStream,
                              ProcessorContext processorContext) throws Exception;
}
