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
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ListProcessor implements ContainerProcessor<Tuple<Integer, String>, Tuple<Integer, String>> {
    public static final AtomicInteger DEBUG_COUNTER = new AtomicInteger(0);
    public static final AtomicInteger DEBUG_COUNTER1 = new AtomicInteger(0);
    private final Map<Integer, Tuple<Integer, String>> list = new HashMap<Integer, Tuple<Integer, String>>();


    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        assert processorContext.getTaskCount() == 1;
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Integer, String>> inputStream,
                           ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        DEBUG_COUNTER.addAndGet(inputStream.size());

        for (Tuple<Integer, String> tuple : inputStream) {
            list.put(tuple.getKey(0), tuple);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        DEBUG_COUNTER1.incrementAndGet();
        System.out.println("Eventually I am writing");

        for (Map.Entry<Integer, Tuple<Integer, String>> integerTupleEntry : list.entrySet()) {
            outputStream.consume(integerTupleEntry.getValue());
        }

        System.out.println("Eventually I've written");

        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {
        list.clear();
    }
}
