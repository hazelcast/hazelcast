/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.stream.IntStream.range;

/**
 * Private API. Access via {@link SourceProcessors#streamJmsQueueP} or {@link
 * SourceProcessors#streamJmsTopicP}
 * <p>
 * Since we use a non-blocking version of JMS consumer API, the processor is
 * marked as cooperative.
 */
public class StreamJmsP<T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 4;

    private final Connection connection;
    private final DistributedFunction<? super Connection, ? extends Session> sessionFn;
    private final DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn;
    private final DistributedConsumer<? super Session> flushFn;
    private final DistributedFunction<? super Message, ? extends T> projectionFn;
    private final WatermarkSourceUtil<? super T> wsu;

    private Session session;
    private MessageConsumer consumer;
    private Traverser<Object> traverser;

    StreamJmsP(Connection connection,
               DistributedFunction<? super Connection, ? extends Session> sessionFn,
               DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn,
               DistributedConsumer<? super Session> flushFn,
               DistributedFunction<? super Message, ? extends T> projectionFn,
               EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.connection = connection;
        this.sessionFn = sessionFn;
        this.consumerFn = consumerFn;
        this.flushFn = flushFn;
        this.projectionFn = projectionFn;

        wsu = new WatermarkSourceUtil<>(eventTimePolicy);
        wsu.increasePartitionCount(1);
    }

    /**
     * Private API. Use {@link SourceProcessors#streamJmsQueueP} or {@link
     * SourceProcessors#streamJmsTopicP} instead.
     */
    @Nonnull
    public static <T> ProcessorSupplier supplier(
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull DistributedFunction<? super Connection, ? extends Session> sessionFn,
            @Nonnull DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<? super Session> flushFn,
            @Nonnull DistributedFunction<? super Message, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        return new Supplier<>(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn, eventTimePolicy);
    }

    @Override
    protected void init(@Nonnull Context context) {
        session = sessionFn.apply(connection);
        consumer = consumerFn.apply(session);
        traverser = ((Traverser<Message>) () -> uncheckCall(() -> consumer.receiveNoWait()))
                .flatMap(t -> wsu.handleEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t)))
                .peek(item -> flushFn.accept(session));
    }

    private long handleJmsTimestamp(Message msg) {
        try {
            // as per `getJMSTimestamp` javadoc, it can return 0 if the timestamp was optimized away
            return msg.getJMSTimestamp() == 0 ? WatermarkSourceUtil.NO_NATIVE_TIME : msg.getJMSTimestamp();
        } catch (JMSException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean complete() {
        emitFromTraverser(traverser);
        return false;
    }

    @Override
    public void close() throws Exception {
        consumer.close();
        session.close();
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final DistributedSupplier<? extends Connection> connectionSupplier;
        private final DistributedFunction<? super Connection, ? extends Session> sessionFn;
        private final DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn;
        private final DistributedConsumer<? super Session> flushFn;
        private final DistributedFunction<? super Message, ? extends T> projectionFn;
        private final EventTimePolicy<? super T> eventTimePolicy;

        private transient Connection connection;

        private Supplier(DistributedSupplier<? extends Connection> connectionSupplier,
                         DistributedFunction<? super Connection, ? extends Session> sessionFn,
                         DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn,
                         DistributedConsumer<? super Session> flushFn,
                         DistributedFunction<? super Message, ? extends T> projectionFn,
                         EventTimePolicy<? super T> eventTimePolicy
        ) {
            this.connectionSupplier = connectionSupplier;
            this.sessionFn = sessionFn;
            this.consumerFn = consumerFn;
            this.flushFn = flushFn;
            this.projectionFn = projectionFn;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            connection = connectionSupplier.get();
            connection.start();
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return range(0, count)
                    .mapToObj(i ->
                            new StreamJmsP<>(connection, sessionFn, consumerFn, flushFn, projectionFn, eventTimePolicy))
                    .collect(Collectors.toList());
        }
    }
}
