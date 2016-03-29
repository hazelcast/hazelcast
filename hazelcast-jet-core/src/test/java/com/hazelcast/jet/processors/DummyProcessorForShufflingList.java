package com.hazelcast.jet.processors;


import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.spi.processor.tuple.TupleContainerProcessorFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class DummyProcessorForShufflingList implements TupleContainerProcessor<Integer, String, Integer, String> {
    public static final AtomicInteger DEBUG_COUNTER = new AtomicInteger(0);

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        DEBUG_COUNTER.addAndGet(inputStream.size());
        outputStream.consumeStream(inputStream);
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new DummyProcessor();
        }
    }
}
