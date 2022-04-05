/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.connector.WriteJmsP;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.XAConnectionFactory;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link Sinks#jmsQueueBuilder} or {@link Sinks#jmsTopicBuilder}.
 *
 * @param <T> type of the items the sink accepts
 *
 * @since Jet 3.0
 */
public final class JmsSinkBuilder<T> {

    private final SupplierEx<ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private boolean exactlyOnce = true;
    private FunctionEx<ConnectionFactory, Connection> connectionFn;
    private BiFunctionEx<Session, T, Message> messageFn;

    private String username;
    private String password;
    private String destinationName;

    /**
     * Use {@link Sinks#jmsQueueBuilder} or {@link Sinks#jmsTopicBuilder}.
     */
    JmsSinkBuilder(@Nonnull SupplierEx<ConnectionFactory> factorySupplier, boolean isTopic) {
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
    @Nonnull
    public JmsSinkBuilder<T> connectionParams(@Nullable String username, @Nullable String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Sets the function which creates a connection given a connection factory.
     * <p>
     * If not provided, the following will be used:
     * <pre>{@code
     *     if (factory instanceof XAConnectionFactory) {
     *         XAConnectionFactory xaFactory = (XAConnectionFactory) factory;
     *         return usernameLocal != null || passwordLocal != null
     *                 ? xaFactory.createXAConnection(usernameLocal, passwordLocal)
     *                 : xaFactory.createXAConnection();
     *     } else {
     *         return usernameLocal != null || passwordLocal != null
     *                 ? factory.createConnection(usernameLocal, passwordLocal)
     *                 : factory.createConnection();
     *     }
     * }</pre>
     *
     * The given function must be stateless.
     */
    @Nonnull
    public JmsSinkBuilder<T> connectionFn(@Nullable FunctionEx<ConnectionFactory, Connection> connectionFn) {
        checkSerializable(connectionFn, "connectionFn");
        this.connectionFn = connectionFn;
        return this;
    }

    /**
     * Sets the name of the destination.
     */
    @Nonnull
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
     * <p>
     * The given function must be stateless.
     */
    @Nonnull
    public JmsSinkBuilder<T> messageFn(@Nullable BiFunctionEx<Session, T, Message> messageFn) {
        checkSerializable(messageFn, "messageFn");
        this.messageFn = messageFn;
        return this;
    }

    /**
     * Enables or disables the exactly-once behavior of the sink using
     * two-phase commit of state snapshots. If enabled, the {@linkplain
     * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
     * guarantee} of the job must be set to {@linkplain
     * ProcessingGuarantee#EXACTLY_ONCE exactly-once}, otherwise the sink's
     * guarantee will match that of the job. In other words, sink's
     * guarantee cannot be higher than job's, but can be lower to avoid the
     * additional overhead.
     *
     * <p>The default value is true.
     *
     * @param enable If true, sink's guarantee will match the job
     *      guarantee. If false, sink's guarantee will be at-least-once
     *      even if job's is exactly-once
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSinkBuilder<T> exactlyOnce(boolean enable) {
        this.exactlyOnce = enable;
        return this;
    }

    /**
     * Creates and returns the JMS {@link Sink} with the supplied components.
     */
    @Nonnull
    public Sink<T> build() {
        String usernameLocal = username;
        String passwordLocal = password;

        checkNotNull(destinationName);
        if (connectionFn == null) {
            connectionFn = factory -> {
                if (factory instanceof XAConnectionFactory) {
                    XAConnectionFactory xaFactory = (XAConnectionFactory) factory;
                    return usernameLocal != null || passwordLocal != null
                            ? xaFactory.createXAConnection(usernameLocal, passwordLocal)
                            : xaFactory.createXAConnection();
                } else {
                    return usernameLocal != null || passwordLocal != null
                            ? factory.createConnection(usernameLocal, passwordLocal)
                            : factory.createConnection();
                }
            };
        }
        if (messageFn == null) {
            messageFn = (session, item) ->
                    item instanceof Message ? (Message) item : session.createTextMessage(item.toString());
        }

        FunctionEx<ConnectionFactory, Connection> connectionFnLocal = connectionFn;
        @SuppressWarnings("UnnecessaryLocalVariable") // it's necessary to not capture this in the lambda
        SupplierEx<ConnectionFactory> factorySupplierLocal = factorySupplier;
        SupplierEx<Connection> newConnectionFn = () -> connectionFnLocal.apply(factorySupplierLocal.get());
        return Sinks.fromProcessor(sinkName(),
                WriteJmsP.supplier(destinationName, exactlyOnce, newConnectionFn, messageFn, isTopic));
    }

    private String sinkName() {
        return String.format("jms%sSink(%s)", isTopic ? "Topic" : "Queue", destinationName);
    }
}
