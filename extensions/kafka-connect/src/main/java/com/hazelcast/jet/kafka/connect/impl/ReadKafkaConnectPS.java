package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class ReadKafkaConnectPS<T> implements ProcessorSupplier {
    private final Properties properties;
    private final EventTimePolicy<? super T> eventTimePolicy;
    private final FunctionEx<SourceRecord, T> projectionFn;
    private transient ConnectorWrapper connectorWrapper;

    public ReadKafkaConnectPS(Properties properties, EventTimePolicy<? super T> eventTimePolicy,
                              FunctionEx<SourceRecord, T> projectionFn) {
        this.properties = properties;
        this.eventTimePolicy = eventTimePolicy;
        this.projectionFn = projectionFn;
    }

    public static <T> ProcessorSupplier kafkaConnectPS(@Nonnull Properties properties,
                                                       @Nonnull EventTimePolicy<? super T> eventTimePolicy,
                                                       @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        return new ReadKafkaConnectPS<>(properties, eventTimePolicy, projectionFn);
    }

    @Override
    public void init(@Nonnull Context context) {
        properties.put("tasks.max", Integer.toString(context.totalParallelism()));

        long jobId = context.jobId();

        this.connectorWrapper = new ConnectorWrapper(jobId, properties);
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (connectorWrapper != null) {
            connectorWrapper.stop();
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return IntStream.range(0, count)
                        .mapToObj(i -> new ReadKafkaConnectP<>(connectorWrapper, eventTimePolicy, projectionFn))
                        .collect(toList());
    }

}
