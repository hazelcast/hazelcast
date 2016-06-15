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

package com.hazelcast.jet.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

public class CountProcessor implements ContainerProcessor<Object, Tuple<Long, Integer>> {
    private int result = 0;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        result = 0;
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<Tuple<Long, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        result += inputStream.size();
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Long, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        System.out.println("result=" + result + " " + processorContext.getNodeEngine().getLocalMember());
        outputStream.consume(new JetTuple2<>(0L, result));
        return true;
    }
}
