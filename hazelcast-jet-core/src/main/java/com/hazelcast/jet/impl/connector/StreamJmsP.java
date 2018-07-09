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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
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
    private final DistributedFunction<Connection, Session> sessionFn;
    private final DistributedFunction<Session, MessageConsumer> consumerFn;
    private final DistributedConsumer<Session> flushFn;
    private final DistributedFunction<Message, T> projectionFn;

    private Session session;
    private MessageConsumer consumer;
    private Traverser<T> traverser;

    StreamJmsP(Connection connection,
               DistributedFunction<Connection, Session> sessionFn,
               DistributedFunction<Session, MessageConsumer> consumerFn,
               DistributedConsumer<Session> flushFn,
               DistributedFunction<Message, T> projectionFn
    ) {
        this.connection = connection;
        this.sessionFn = sessionFn;
        this.consumerFn = consumerFn;
        this.flushFn = flushFn;
        this.projectionFn = projectionFn;
    }

    /**
     * Private API. Use {@link SourceProcessors#streamJmsQueueP} or {@link
     * SourceProcessors#streamJmsTopicP} instead.
     */
    @Nonnull
    public static <T> ProcessorSupplier supplier(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Session> sessionFn,
            @Nonnull DistributedFunction<Session, MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<Session> flushFn,
            @Nonnull DistributedFunction<Message, T> projectionFn
    ) {
        return new Supplier<>(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn);
    }

    @Override
    protected void init(@Nonnull Context context) {
        session = sessionFn.apply(connection);
        consumer = consumerFn.apply(session);
        traverser = ((Traverser<Message>) () -> uncheckCall(() -> consumer.receiveNoWait()))
                .map(projectionFn)
                .peek(item -> flushFn.accept(session));
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

        private final DistributedSupplier<Connection> connectionSupplier;
        private final DistributedFunction<Connection, Session> sessionFn;
        private final DistributedFunction<Session, MessageConsumer> consumerFn;
        private final DistributedConsumer<Session> flushFn;
        private final DistributedFunction<Message, T> projectionFn;

        private transient Connection connection;

        private Supplier(DistributedSupplier<Connection> connectionSupplier,
                         DistributedFunction<Connection, Session> sessionFn,
                         DistributedFunction<Session, MessageConsumer> consumerFn,
                         DistributedConsumer<Session> flushFn,
                         DistributedFunction<Message, T> projectionFn) {
            this.connectionSupplier = connectionSupplier;
            this.sessionFn = sessionFn;
            this.consumerFn = consumerFn;
            this.flushFn = flushFn;
            this.projectionFn = projectionFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            connection = connectionSupplier.get();
            uncheckRun(() -> connection.start());
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
                    .mapToObj(i -> new StreamJmsP<>(connection, sessionFn, consumerFn, flushFn, projectionFn))
                    .collect(Collectors.toList());
        }
    }
}
