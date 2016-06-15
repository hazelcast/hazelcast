package com.hazelcast.jet.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.processor.ContainerProcessor;

public class DummyEmittingProcessor implements ContainerProcessor<Tuple<Integer, String>, Tuple<Integer, String>> {

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        System.out.println("finalize");
        outputStream.consume(new JetTuple2<>(0, "empty"));
        return true;
    }
}


