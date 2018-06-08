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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * See {@link Sources#jmsQueueBuilder} of {@link Sources#jmsTopicBuilder}.
 *
 * @param <T> type of the items the source emits
 */
public final class JmsSourceBuilder<T> {

    private final DistributedSupplier<ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private DistributedFunction<ConnectionFactory, Connection> connectionFn;
    private DistributedFunction<Connection, Session> sessionFn;
    private DistributedFunction<Session, MessageConsumer> consumerFn;
    private DistributedFunction<Message, T> projectionFn;
    private DistributedConsumer<Session> flushFn;

    private String username;
    private String password;
    private boolean transacted;
    private int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private String destinationName;

    /**
     * Use {@link Sources#jmsQueueBuilder} of {@link Sources#jmsTopicBuilder}.
     */
    JmsSourceBuilder(DistributedSupplier<ConnectionFactory> factorySupplier, boolean isTopic) {
        checkSerializable(factorySupplier, "factorySupplier");
        this.factorySupplier = factorySupplier;
        this.isTopic = isTopic;
    }

    /**
     * Sets the connection parameters. If {@code connectionFn} is provided these
     * parameters are ignored.
     *
     * @param username   the username, Default value is {@code null}
     * @param password   the password, Default value is {@code null}
     */
    public JmsSourceBuilder<T> connectionParams(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Sets the function which creates the connection from connection factory.
     * <p>
     * If not provided, the builder creates a function which uses {@code
     * ConnectionFactory#createConnection(username, password)} to create the
     * connection. See {@link #connectionParams(String, String)}.
     */
    public JmsSourceBuilder<T> connectionFn(@Nonnull DistributedFunction<ConnectionFactory, Connection> connectionFn) {
        checkSerializable(connectionFn, "connectionFn");
        this.connectionFn = connectionFn;
        return this;
    }

    /**
     * Sets the session parameters. If {@code sessionFn} is provided these
     * parameters are ignored.
     *
     * @param transacted       if true marks the session as transacted false otherwise,
     *                         Default value is false.
     * @param acknowledgeMode  sets the acknowledge mode of the session,
     *                         Default value is {@code Session.AUTO_ACKNOWLEDGE}
     */
    public JmsSourceBuilder<T> sessionParams(boolean transacted, int acknowledgeMode) {
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        return this;
    }

    /**
     * Sets the function which creates the session from connection.
     * <p>
     * If not provided, the builder creates a function which uses {@code
     * Connection#createSession(boolean transacted, int acknowledgeMode)} to
     * create the session. See {@link #sessionParams(boolean, int)}.
     */
    public JmsSourceBuilder<T> sessionFn(@Nonnull DistributedFunction<Connection, Session> sessionFn) {
        checkSerializable(sessionFn, "sessionFn");
        this.sessionFn = sessionFn;
        return this;
    }

    /**
     * Sets the name of the destination. If {@code consumerFn} is provided this
     * parameter is ignored.
     */
    public JmsSourceBuilder<T> destinationName(String destinationName) {
        this.destinationName = destinationName;
        return this;
    }

    /**
     * Sets the function which creates the message consumer from session.
     * <p>
     * If not provided, the builder creates a function which uses {@code
     * Session#createConsumer(Destination destination)} to create the consumer.
     * Either {@code consumerFn} or {@code destinationName} should be set. See
     * {@link #destinationName(String)}.
     */
    public JmsSourceBuilder<T> consumerFn(@Nonnull DistributedFunction<Session, MessageConsumer> consumerFn) {
        checkSerializable(consumerFn, "consumerFn");
        this.consumerFn = consumerFn;
        return this;
    }

    /**
     * Sets the function which creates output object from {@code Message}.
     * <p>
     * If not provided, the builder creates an identity function.
     */
    public JmsSourceBuilder<T> projectionFn(DistributedFunction<Message, T> projectionFn) {
        checkSerializable(projectionFn, "projectionFn");
        this.projectionFn = projectionFn;
        return this;
    }

    /**
     * Sets the function which commits the session after consuming each message.
     * <p>
     * If not provided, the builder creates a no-op consumer.
     */
    public JmsSourceBuilder<T> flushFn(DistributedConsumer<Session> flushFn) {
        checkSerializable(flushFn, "flushFn");
        this.flushFn = flushFn;
        return this;
    }

    /**
     * Creates and returns the JMS {@link StreamSource} with the supplied
     * components.
     */
    public StreamSource<T> build() {
        String usernameLocal = username;
        String passwordLocal = password;
        boolean transactedLocal = transacted;
        int acknowledgeModeLocal = acknowledgeMode;
        String nameLocal = destinationName;
        boolean isTopicLocal = isTopic;

        if (connectionFn == null) {
            connectionFn = factory -> uncheckCall(() -> factory.createConnection(usernameLocal, passwordLocal));
        }
        if (sessionFn == null) {
            sessionFn = connection -> uncheckCall(() -> connection.createSession(transactedLocal, acknowledgeModeLocal));
        }
        if (consumerFn == null) {
            checkNotNull(nameLocal);
            consumerFn = session -> uncheckCall(() -> {
                Destination destination = isTopicLocal ? session.createTopic(nameLocal) : session.createQueue(nameLocal);
                return session.createConsumer(destination);
            });
        }
        if (projectionFn == null) {
            projectionFn = m -> (T) m;
        }
        if (flushFn == null) {
            flushFn = noopConsumer();
        }

        DistributedFunction<ConnectionFactory, Connection> connectionFnLocal = connectionFn;
        DistributedSupplier<ConnectionFactory> factorySupplierLocal = factorySupplier;
        DistributedSupplier<Connection> connectionSupplier = () -> connectionFnLocal.apply(factorySupplierLocal.get());

        ProcessorMetaSupplier metaSupplier = isTopic ?
                SourceProcessors.streamJmsTopicP(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn)
                : SourceProcessors.streamJmsQueueP(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn);

        return new StreamSourceTransform<>(sourceName(), w -> metaSupplier, false);
    }

    private String sourceName() {
        return (isTopic ? "jmsTopicSource(" : "jmsQueueSource(")
                + (destinationName == null ? "?" : destinationName) + ")";
    }
}
