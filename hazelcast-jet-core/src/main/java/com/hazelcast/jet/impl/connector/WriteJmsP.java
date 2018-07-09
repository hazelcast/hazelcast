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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Collection;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;

/**
 * Private API. Access via {@link SinkProcessors#writeJmsQueueP} or {@link
 * SinkProcessors#writeJmsTopicP}.
 */
public final class WriteJmsP {

    private static final int PREFERRED_LOCAL_PARALLELISM = 4;

    private WriteJmsP() {
    }

    /**
     * Private API. Use {@link SinkProcessors#writeJmsQueueP} or {@link
     * SinkProcessors#writeJmsTopicP} instead
     */
    public static <T> ProcessorMetaSupplier supplier(
            DistributedSupplier<Connection> connectionSupplier,
            DistributedFunction<Connection, Session> sessionF,
            DistributedBiFunction<Session, T, Message> messageFn,
            DistributedBiConsumer<MessageProducer, Message> sendFn,
            DistributedConsumer<Session> flushFn,
            String name,
            boolean isTopic
    ) {
        return ProcessorMetaSupplier.of(
                new Supplier<>(connectionSupplier, sessionF, messageFn, sendFn, flushFn, name, isTopic),
                PREFERRED_LOCAL_PARALLELISM);
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final DistributedSupplier<Connection> connectionSupplier;
        private final DistributedFunction<Connection, Session> sessionF;
        private final String name;
        private final boolean isTopic;
        private final DistributedBiFunction<Session, T, Message> messageFn;
        private final DistributedBiConsumer<MessageProducer, Message> sendFn;
        private final DistributedConsumer<Session> flushFn;

        private transient Connection connection;

        private Supplier(DistributedSupplier<Connection> connectionSupplier,
                         DistributedFunction<Connection, Session> sessionF,
                         DistributedBiFunction<Session, T, Message> messageFn,
                         DistributedBiConsumer<MessageProducer, Message> sendFn,
                         DistributedConsumer<Session> flushFn,
                         String name,
                         boolean isTopic
        ) {
            this.connectionSupplier = connectionSupplier;
            this.sessionF = sessionF;
            this.messageFn = messageFn;
            this.sendFn = sendFn;
            this.flushFn = flushFn;
            this.name = name;
            this.isTopic = isTopic;
        }

        @Override
        public void init(Context ignored) {
            connection = connectionSupplier.get();
            uncheckRun(() -> connection.start());
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            DistributedFunction<Processor.Context, JmsContext> createFn = jet -> {
                Session session = sessionF.apply(connection);
                Destination destination = isTopic ? session.createTopic(name) : session.createQueue(name);
                MessageProducer producer = session.createProducer(destination);
                return new JmsContext(session, producer);
            };
            DistributedBiConsumer<JmsContext, T> onReceiveFn = (buffer, item) -> {
                Message message = messageFn.apply(buffer.session, item);
                sendFn.accept(buffer.producer, message);
            };
            DistributedConsumer<JmsContext> flushF = buffer -> flushFn.accept(buffer.session);
            DistributedConsumer<JmsContext> destroyFn = buffer -> {
                buffer.producer.close();
                buffer.session.close();
            };
            DistributedSupplier<Processor> supplier = writeBufferedP(createFn, onReceiveFn, flushF, destroyFn);

            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void close(Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        class JmsContext {
            private final Session session;
            private final MessageProducer producer;

            JmsContext(Session session, MessageProducer producer) {
                this.session = session;
                this.producer = producer;
            }
        }
    }
}
