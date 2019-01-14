/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import java.util.function.Function;

import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsQueueP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsTopicP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * See {@link Sources#jmsQueueBuilder} or {@link Sources#jmsTopicBuilder}.
 */
public final class JmsSourceBuilder {

    private final DistributedSupplier<? extends ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private DistributedFunction<? super ConnectionFactory, ? extends Connection> connectionFn;
    private DistributedFunction<? super Connection, ? extends Session> sessionFn;
    private DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn;
    private DistributedConsumer<? super Session> flushFn;

    private String username;
    private String password;
    private boolean transacted;
    private int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private String destinationName;

    /**
     * Use {@link Sources#jmsQueueBuilder} of {@link Sources#jmsTopicBuilder}.
     */
    JmsSourceBuilder(DistributedSupplier<? extends ConnectionFactory> factorySupplier, boolean isTopic) {
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
    public JmsSourceBuilder connectionParams(String username, String password) {
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
    public JmsSourceBuilder connectionFn(
            @Nonnull DistributedFunction<? super ConnectionFactory, ? extends Connection> connectionFn
    ) {
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
    public JmsSourceBuilder sessionParams(boolean transacted, int acknowledgeMode) {
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
    public JmsSourceBuilder sessionFn(@Nonnull DistributedFunction<? super Connection, ? extends Session> sessionFn) {
        checkSerializable(sessionFn, "sessionFn");
        this.sessionFn = sessionFn;
        return this;
    }

    /**
     * Sets the name of the destination. If {@code consumerFn} is provided this
     * parameter is ignored.
     */
    public JmsSourceBuilder destinationName(String destinationName) {
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
    public JmsSourceBuilder consumerFn(
            @Nonnull DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn
    ) {
        checkSerializable(consumerFn, "consumerFn");
        this.consumerFn = consumerFn;
        return this;
    }

    /**
     * Sets the function which commits the session after consuming each message.
     * <p>
     * If not provided, the builder creates a no-op consumer.
     */
    public JmsSourceBuilder flushFn(@Nonnull DistributedConsumer<? super Session> flushFn) {
        checkSerializable(flushFn, "flushFn");
        this.flushFn = flushFn;
        return this;
    }

    /**
     * Creates and returns the JMS {@link StreamSource} with the supplied
     * components and the projection function {@code projectionFn}.
     *
     * @param projectionFn the function which creates output object from each
     *                    message
     * @param <T> the type of the items the source emits
     */
    public <T> StreamSource<T> build(@Nonnull DistributedFunction<? super Message, ? extends T> projectionFn) {
        String usernameLocal = username;
        String passwordLocal = password;
        boolean transactedLocal = transacted;
        int acknowledgeModeLocal = acknowledgeMode;
        String nameLocal = destinationName;
        @SuppressWarnings("UnnecessaryLocalVariable")
        boolean isTopicLocal = isTopic;

        if (connectionFn == null) {
            connectionFn = factory -> factory.createConnection(usernameLocal, passwordLocal);
        }
        if (sessionFn == null) {
            sessionFn = connection -> connection.createSession(transactedLocal, acknowledgeModeLocal);
        }
        if (consumerFn == null) {
            checkNotNull(nameLocal);
            consumerFn = session -> session.createConsumer(isTopicLocal
                    ? session.createTopic(nameLocal)
                    : session.createQueue(nameLocal));
        }
        if (flushFn == null) {
            flushFn = DistributedConsumer.noop();
        }

        DistributedFunction<? super ConnectionFactory, ? extends Connection> connectionFnLocal = connectionFn;
        @SuppressWarnings("UnnecessaryLocalVariable")
        DistributedSupplier<? extends ConnectionFactory> factorySupplierLocal = factorySupplier;
        DistributedSupplier<? extends Connection> connectionSupplier =
                () -> connectionFnLocal.apply(factorySupplierLocal.get());

        Function<EventTimePolicy<? super T>, ProcessorMetaSupplier> metaSupplierFactory = policy ->
                isTopic ? streamJmsTopicP(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn, policy)
                        : streamJmsQueueP(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn, policy);
        return Sources.streamFromProcessorWithWatermarks(sourceName(), metaSupplierFactory, true);
    }

    /**
     * Convenience for {@link JmsSourceBuilder#build(DistributedFunction)}.
     */
    public StreamSource<Message> build() {
        return build(message -> message);
    }

    private String sourceName() {
        return (isTopic ? "jmsTopicSource(" : "jmsQueueSource(")
                + (destinationName == null ? "?" : destinationName) + ')';
    }
}
