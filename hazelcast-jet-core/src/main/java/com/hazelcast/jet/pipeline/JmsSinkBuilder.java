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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.WriteJmsP;
import com.hazelcast.jet.impl.pipeline.SinkImpl;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * See {@link Sinks#jmsQueueBuilder} or {@link Sinks#jmsTopicBuilder}.
 *
 * @param <T> type of the items the sink accepts
 */
public final class JmsSinkBuilder<T> {

    private final DistributedSupplier<ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private DistributedFunction<ConnectionFactory, Connection> connectionFn;
    private DistributedFunction<Connection, Session> sessionFn;
    private DistributedBiFunction<Session, T, Message> messageFn;
    private DistributedBiConsumer<MessageProducer, Message> sendFn;
    private DistributedConsumer<Session> flushFn;

    private String username;
    private String password;
    private boolean transacted;
    private int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private String destinationName;

    /**
     * Use {@link Sinks#jmsQueueBuilder} or {@link Sinks#jmsTopicBuilder}.
     */
    JmsSinkBuilder(@Nonnull DistributedSupplier<ConnectionFactory> factorySupplier, boolean isTopic) {
        checkSerializable(factorySupplier, "factorySupplier");
        this.factorySupplier = factorySupplier;
        this.isTopic = isTopic;
    }

    /**
     * Sets the connection parameters. If {@code connectionFn} is provided, these
     * parameters are ignored.
     *
     * @param username   the username, Default value is {@code null}
     * @param password   the password, Default value is {@code null}
     */
    public JmsSinkBuilder<T> connectionParams(String username, String password) {
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
    public JmsSinkBuilder<T> connectionFn(@Nonnull DistributedFunction<ConnectionFactory, Connection> connectionFn) {
        checkSerializable(connectionFn, "connectionFn");
        this.connectionFn = connectionFn;
        return this;
    }

    /**
     * Sets the session parameters. If {@code sessionFn} is provided, these
     * parameters are ignored.
     *
     * @param transacted       if true, marks the session as transacted.
     *                         Default value is false.
     * @param acknowledgeMode  sets the acknowledge mode of the session,
     *                         Default value is {@code Session.AUTO_ACKNOWLEDGE}
     */
    public JmsSinkBuilder<T> sessionParams(boolean transacted, int acknowledgeMode) {
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        return this;
    }

    /**
     * Sets the function which creates a session from a connection.
     * <p>
     * If not provided, the builder creates a function which uses {@code
     * Connection#createSession(boolean transacted, int acknowledgeMode)} to
     * create the session. See {@link #sessionParams(boolean, int)}.
     */
    public JmsSinkBuilder<T> sessionFn(@Nonnull DistributedFunction<Connection, Session> sessionFn) {
        checkSerializable(sessionFn, "sessionFn");
        this.sessionFn = sessionFn;
        return this;
    }

    /**
     * Sets the name of the destination.
     */
    public JmsSinkBuilder<T> destinationName(@Nonnull String destinationName) {
        this.destinationName = destinationName;
        return this;
    }

    /**
     * Sets the function which creates the message from the item.
     * <p>
     * If not provided, the builder creates a function which wraps {@code
     * item.toString()} into a {@link javax.jms.TextMessage}, unless the item
     * is already an instance of {@code javax.jms.Message}.
     */
    public JmsSinkBuilder<T> messageFn(DistributedBiFunction<Session, T, Message> messageFn) {
        checkSerializable(messageFn, "messageFn");
        this.messageFn = messageFn;
        return this;
    }

    /**
     * Sets the function which sends the message via message producer.
     * <p>
     * If not provided, the builder creates a function which sends the message via
     * {@code MessageProducer#send(Message message)}.
     */
    public JmsSinkBuilder<T> sendFn(DistributedBiConsumer<MessageProducer, Message> sendFn) {
        checkSerializable(sendFn, "sendFn");
        this.sendFn = sendFn;
        return this;
    }

    /**
     * Sets the function which flushes the session after a batch of messages is
     * sent.
     * <p>
     * If not provided, the builder creates a no-op consumer.
     */
    public JmsSinkBuilder<T> flushFn(DistributedConsumer<Session> flushFn) {
        checkSerializable(flushFn, "flushFn");
        this.flushFn = flushFn;
        return this;
    }

    /**
     * Creates and returns the JMS {@link Sink} with the supplied components.
     */
    public Sink<T> build() {
        String usernameLocal = username;
        String passwordLocal = password;
        boolean transactedLocal = transacted;
        int acknowledgeModeLocal = acknowledgeMode;

        checkNotNull(destinationName);
        if (connectionFn == null) {
            connectionFn = factory -> factory.createConnection(usernameLocal, passwordLocal);
        }
        if (sessionFn == null) {
            sessionFn = connection -> connection.createSession(transactedLocal, acknowledgeModeLocal);
        }
        if (messageFn == null) {
            messageFn = (session, item) ->
                    item instanceof Message ? (Message) item : session.createTextMessage(item.toString());
        }
        if (sendFn == null) {
            sendFn = MessageProducer::send;
        }
        if (flushFn == null) {
            flushFn = noopConsumer();
        }

        DistributedFunction<ConnectionFactory, Connection> connectionFnLocal = connectionFn;
        DistributedSupplier<ConnectionFactory> factorySupplierLocal = factorySupplier;
        DistributedSupplier<Connection> connectionSupplier = () -> connectionFnLocal.apply(factorySupplierLocal.get());
        return new SinkImpl<>(sinkName(),
                WriteJmsP.supplier(connectionSupplier, sessionFn, messageFn, sendFn, flushFn, destinationName, isTopic));
    }

    private String sinkName() {
        return String.format("jms%sSink(%s)", isTopic ? "Topic" : "Queue", destinationName);
    }
}
