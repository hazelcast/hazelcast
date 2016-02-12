package com.hazelcast.jet.processors;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ContainerProcessor;

public class CounterProcessor implements ContainerProcessor<Object, Object> {
    private final CounterKey OBJECTS_COUNTER = new StringCounterKey("counter");
    private LongCounter longCounter;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        this.longCounter = new LongCounter();
        processorContext.setAccumulator(OBJECTS_COUNTER, this.longCounter);
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<Object> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        this.longCounter.add(inputStream.size());
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Object> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    private static class StringCounterKey implements CounterKey {
        private final String counter;

        private StringCounterKey(String counter) {
            this.counter = counter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringCounterKey that = (StringCounterKey) o;

            return counter.equals(that.counter);
        }

        @Override
        public int hashCode() {
            return counter.hashCode();
        }
    }

    public static class CounterProcessorFactory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new CounterProcessor();
        }
    }
}
