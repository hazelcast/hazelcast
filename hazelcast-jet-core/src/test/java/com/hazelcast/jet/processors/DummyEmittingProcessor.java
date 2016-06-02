package com.hazelcast.jet.processors;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.tuple.JetTuple2;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessorFactory;

public class DummyEmittingProcessor implements TupleContainerProcessor<Integer, String, Integer, String> {
    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        System.out.println("finalize");
        outputStream.consume(new JetTuple2<>(0, "empty"));
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new DummyEmittingProcessor();
        }
    }
}


