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
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * See {@link JmsSinkBuilder#builder(DistributedSupplier)}
 *
 * @param <T> type of the items the sink accepts
 */
public final class JmsSinkBuilder<T> {

    private final DistributedSupplier<ConnectionFactory> factorySupplier;

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
    private boolean isTopic;

    /**
     * Use {@link JmsSinkBuilder#builder(DistributedSupplier)}.
     */
    private JmsSinkBuilder(@Nonnull DistributedSupplier<ConnectionFactory> factorySupplier) {
        this.factorySupplier = factorySupplier;
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom JMS {@link StreamSource} for the Pipeline API.
     * <p>
     * These are the callback functions you can provide to implement the sink's
     * behavior:
     * <ol><li>
     *     {@code factorySupplier} creates the connection factory. This
     *     component is required.
     * </li><li>
     *     {@code destinationName} sets the name of the destination. This
     *     component is required.
     * </li><li>
     *     {@code connectionFn} creates the connection. This component is
     *     optional; if not provided, the builder creates a function which uses
     *     {@code ConnectionFactory#createConnection(username, password)} to
     *     create the connection. See {@link #connectionParams(String, String)}.
     * </li><li>
     *     {@code sessionFn} creates the session. This component is optional;
     *     if not provided, the builder creates a function which uses {@code
     *     Connection#createSession(boolean transacted, int acknowledgeMode)}
     *     to create the session. See {@link #sessionParams(boolean, int)}.
     * </li><li>
     *     {@code messageFn} creates the message from the item. This component is
     *     optional; if not provided, the builder creates a function that wraps {@code
     *     item.toString()} into a {@link javax.jms.TextMessage}.
     * </li><li>
     *     {@code sendFn} sends the message via message producer. This component
     *     is optional; if not provided, the builder creates a function which sends
     *     the message using {@code MessageProducer#send(Message message)}.
     * </li><li>
     *     {@code flushFn} flushes the session. This component is optional; if
     *     not provided, the builder creates a no-op consumer.
     * </li><li>
     *     {@code topic} sets that the destination is a topic. This call is
     *     optional; if not called, the builder treats the destination as a queue.
     * </li></ol>
     *
     * @param <T> type of the items the source emits
     */
    public static <T> JmsSinkBuilder<T> builder(@Nonnull DistributedSupplier<ConnectionFactory> factorySupplier) {
        return new JmsSinkBuilder<>(factorySupplier);
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
     * Sets that the destination is a topic. If not called, the destination
     * will be treated as a queue.
     */
    public JmsSinkBuilder<T> topic() {
        this.isTopic = true;
        return this;
    }

    /**
     * Sets the function which creates the message from the item.
     * <p>
     * If not provided, the builder creates a function which wraps {@code
     * item.toString()} into a {@link javax.jms.TextMessage}.
     */
    public JmsSinkBuilder<T> messageFn(DistributedBiFunction<Session, T, Message> messageFn) {
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
            connectionFn = factory -> uncheckCall(() -> factory.createConnection(usernameLocal, passwordLocal));
        }
        if (sessionFn == null) {
            sessionFn = connection -> uncheckCall(() -> connection.createSession(transactedLocal, acknowledgeModeLocal));
        }
        if (messageFn == null) {
            messageFn = (session, item) -> uncheckCall(() -> session.createTextMessage(item.toString()));
        }
        if (sendFn == null) {
            sendFn = (producer, message) -> uncheckRun(() -> producer.send(message));
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
        return String.format("jms%s(%s)", isTopic ? "Topic" : "Queue", destinationName);
    }


}
