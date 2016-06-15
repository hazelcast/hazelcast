package com.hazelcast.jet.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.concurrent.atomic.AtomicInteger;

public class DummyProcessorForShufflingList implements ContainerProcessor<Tuple<Integer, String>, Tuple<Integer, String>> {
    public static final AtomicInteger DEBUG_COUNTER = new AtomicInteger(0);


    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        DEBUG_COUNTER.addAndGet(inputStream.size());
        outputStream.consumeStream(inputStream);
        return true;
    }
}
