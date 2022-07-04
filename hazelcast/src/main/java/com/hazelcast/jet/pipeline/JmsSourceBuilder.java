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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsQueueP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsTopicP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * See {@link Sources#jmsQueueBuilder} or {@link Sources#jmsTopicBuilder}.
 *
 * @since Jet 3.0
 */
public final class JmsSourceBuilder {

    private final SupplierEx<? extends ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFn;
    private FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
    private FunctionEx<? super Message, ?> messageIdFn = Message::getJMSMessageID;

    private String username;
    private String password;
    private String destinationName;
    private ProcessingGuarantee maxGuarantee = ProcessingGuarantee.EXACTLY_ONCE;
    private boolean isSharedConsumer;

    /**
     * Use {@link Sources#jmsQueueBuilder} of {@link Sources#jmsTopicBuilder}.
     * <p>
     * The given function must be stateless.
     */
    JmsSourceBuilder(@Nonnull SupplierEx<? extends ConnectionFactory> factorySupplier, boolean isTopic) {
        checkSerializable(factorySupplier, "factorySupplier");
        this.factorySupplier = checkNotNull(factorySupplier);
        this.isTopic = isTopic;
    }

    /**
     * Sets the connection parameters. If {@link #connectionFn(FunctionEx)} is
     * set, these parameters are ignored.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder connectionParams(@Nullable String username, @Nullable String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Sets the function which creates the connection using the connection
     * factory.
     * <p>
     * If not provided, this function is used:
     * <pre>
     *     connectionFn = factory -> username != null || password != null
     *         ? factory.createConnection(usernameLocal, passwordLocal)
     *         : factory.createConnection()
     * </pre>
     * The user name and password set with {@link #connectionParams} are used.
     * <p>
     * The given function must be stateless.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder connectionFn(
            @Nullable FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFn
    ) {
        checkSerializable(connectionFn, "connectionFn");
        this.connectionFn = connectionFn;
        return this;
    }

    /**
     * Sets the name of the destination (name of the topic or queue). If {@link
     * #consumerFn(FunctionEx)} is provided, this parameter is ignored.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder destinationName(@Nullable String destinationName) {
        this.destinationName = destinationName;
        return this;
    }

    /**
     * Sets the function which creates the message consumer from session.
     * <p>
     * If not provided, {@code Session#createConsumer(destinationName)} is used
     * to create the consumer. See {@link #destinationName(String)}.
     * <p>
     * If you're consuming a topic and you create a shared consumer, make
     * sure to also call {@link #sharedConsumer(boolean) sharedConsumer(true)}.
     * <p>
     * The given function must be stateless.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder consumerFn(
            @Nullable FunctionEx<? super Session, ? extends MessageConsumer> consumerFn
    ) {
        checkSerializable(consumerFn, "consumerFn");
        this.consumerFn = consumerFn;
        return this;
    }

    /**
     * Configures the function to extract IDs from the messages, if
     * exactly-once guarantee is used. If a lower guarantee is used, this
     * function is not used.
     * <p>
     * Make sure the function returns non-null for every message, or the job
     * will fail. The returned object should also implement {@code equals()}
     * and {@code hashCode()} methods. If you don't have a unique message ID,
     * {@linkplain #maxGuarantee(ProcessingGuarantee) reduce the guarantee} to
     * at-least-once.
     * <p>
     * The default is to use {@code Message.getJMSMessageID()}.
     * <p>
     * The given function must be stateless.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder messageIdFn(@Nonnull FunctionEx<? super Message, ?> messageIdFn) {
        this.messageIdFn = checkNotNull(messageIdFn);
        return this;
    }

    /**
     * Sets the maximum processing guarantee for the source. You can use it to
     * reduce the guarantee of this source compared to the job's guarantee. If
     * you configure a stronger guarantee than the job has, the job's guarantee
     * will be used. Use it if you want to avoid the overhead of acknowledging
     * the messages or storing IDs of seen messages, if you can tolerate
     * duplicated or missed messages.
     * <p>
     * If the processing guarantee is NONE, the processor will consume the
     * messages in {@link Session#DUPS_OK_ACKNOWLEDGE} mode. If the processing
     * guarantee is other than NONE, the processor will acknowledge messages in
     * transactions in the 2nd phase of the snapshot, that is after all
     * downstream stages fully processed the messages. Additionally, if the
     * processing guarantee is EXACTLY_ONCE, the processor will store
     * {@linkplain #messageIdFn(FunctionEx) message IDs} of the unacknowledged
     * messages to the snapshot and should the job fail after the snapshot was
     * successful, but before Jet managed to acknowledge the messages. The
     * stored IDs will be used to filter out the re-delivered messages.
     * <p>
     * If you use a non-durable consumer with a topic, the guarantee will not
     * work since the broker doesn't store the messages at all. You can also
     * set the max-guarantee to NONE in this case - the acknowledge operation
     * is ignored anyway. If you didn't specify your own {@link
     * #consumerFn(FunctionEx)}, a non-durable consumer is created for a topic
     * by default.
     * <p>
     * The default is {@link ProcessingGuarantee#EXACTLY_ONCE}, which means
     * that the source's guarantee will match the job's guarantee.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder maxGuarantee(@Nonnull ProcessingGuarantee guarantee) {
        maxGuarantee = checkNotNull(guarantee);
        return this;
    }

    /**
     * Specifies whether the MessageConsumer of the JMS topic is shared, that
     * is whether {@code createSharedConsumer()} or {@code
     * createSharedDurableConsumer()} was used to create it in the {@link
     * #consumerFn(FunctionEx)}.
     * <p>
     * If the consumer is not shared, only a single processor on a single
     * member will connect to the broker to receive the messages. If you set
     * this parameter to {@code true} for a non-shared consumer, all messages
     * will be emitted on every member, leading to duplicate processing.
     * <p>
     * A consumer for a queue is always assumed to be shared, regardless of
     * this setting.
     * <p>
     * The default value is {@code false}.
     *
     * @return this instance for fluent API
     */
    @Nonnull
    public JmsSourceBuilder sharedConsumer(boolean isSharedConsumer) {
        this.isSharedConsumer = isSharedConsumer;
        return this;
    }

    /**
     * Creates and returns the JMS {@link StreamSource} with the supplied
     * components and the projection function {@code projectionFn}.
     * <p>
     * The given function must be stateless.
     *
     * @param projectionFn the function which creates output object from each
     *                    message
     * @param <T> the type of the items the source emits
     */
    @Nonnull
    public <T> StreamSource<T> build(@Nonnull FunctionEx<? super Message, ? extends T> projectionFn) {
        String usernameLocal = username;
        String passwordLocal = password;
        String destinationLocal = destinationName;
        ProcessingGuarantee maxGuaranteeLocal = maxGuarantee;
        @SuppressWarnings("UnnecessaryLocalVariable")
        boolean isTopicLocal = isTopic;

        if (connectionFn == null) {
            connectionFn = factory -> requireNonNull(usernameLocal != null || passwordLocal != null
                    ? factory.createConnection(usernameLocal, passwordLocal)
                    : factory.createConnection());
        }
        if (consumerFn == null) {
            checkNotNull(destinationLocal, "neither consumerFn nor destinationName set");
            consumerFn = session -> session.createConsumer(isTopicLocal
                    ? session.createTopic(destinationLocal)
                    : session.createQueue(destinationLocal));
            if (isTopic) {
                // the user didn't specify a custom consumerFn and we know we're using a non-durable consumer
                // for a topic - there's no point in using any guarantee, see `maxGuarantee`
                maxGuaranteeLocal = ProcessingGuarantee.NONE;
            }
        }

        ProcessingGuarantee maxGuaranteeFinal = maxGuaranteeLocal;

        FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFnLocal = connectionFn;
        @SuppressWarnings("UnnecessaryLocalVariable")
        SupplierEx<? extends ConnectionFactory> factorySupplierLocal = factorySupplier;
        SupplierEx<? extends Connection> newConnectionFn =
                () -> connectionFnLocal.apply(factorySupplierLocal.get());

        FunctionEx<? super Session, ? extends MessageConsumer> consumerFnLocal = consumerFn;
        boolean isSharedConsumerLocal = isSharedConsumer;
        FunctionEx<? super Message, ?> messageIdFnLocal = messageIdFn;

        FunctionEx<EventTimePolicy<? super T>, ProcessorMetaSupplier> metaSupplierFactory =
                policy -> isTopicLocal
                        ? streamJmsTopicP(destinationLocal, isSharedConsumerLocal, maxGuaranteeFinal, policy,
                        newConnectionFn, consumerFnLocal, messageIdFnLocal, projectionFn)
                        : streamJmsQueueP(destinationLocal, maxGuaranteeFinal, policy, newConnectionFn, consumerFnLocal,
                        messageIdFnLocal, projectionFn);
        return Sources.streamFromProcessorWithWatermarks(sourceName(), true, metaSupplierFactory);
    }

    /**
     * Convenience for {@link JmsSourceBuilder#build(FunctionEx)}.
     */
    @Nonnull
    public StreamSource<Message> build() {
        return build(message -> message);
    }

    private String sourceName() {
        return (isTopic ? "jmsTopicSource(" : "jmsQueueSource(")
                + (destinationName == null ? "?" : destinationName) + ')';
    }
}
