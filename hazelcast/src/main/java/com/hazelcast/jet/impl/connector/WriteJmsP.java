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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Private API. Access via {@link SinkProcessors#writeJmsQueueP} or {@link
 * SinkProcessors#writeJmsTopicP}.
 */
public final class WriteJmsP<T> extends XaSinkProcessorBase {

    private static final int PREFERRED_LOCAL_PARALLELISM = 1;
    private final Connection connection;
    private Session session;
    private final String destinationName;
    private final BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn;
    private final boolean isTopic;
    private MessageProducer producer;

    private WriteJmsP(
            Connection connection,
            String destinationName,
            boolean exactlyOnce,
            BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn,
            boolean isTopic
    ) {
        super(exactlyOnce ? EXACTLY_ONCE : AT_LEAST_ONCE);
        this.connection = connection;
        this.destinationName = destinationName;
        this.messageFn = messageFn;
        this.isTopic = isTopic;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        super.init(outbox, context);

        if (snapshotUtility.usesTransactionLifecycle()) {
            XASession xaSession = ((XAConnection) connection).createXASession();
            setXaResource(xaSession.getXAResource());
            session = xaSession;
        } else {
            session = connection.createSession(true, 0);
        }
        Destination destination = isTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);
        producer = session.createProducer(destination);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (snapshotUtility.activeTransaction() == null) {
            return;
        }

        try {
            for (Object item; (item = inbox.poll()) != null; ) {
                @SuppressWarnings("unchecked")
                T castItem = (T) item;
                Message msg = messageFn.apply(session, castItem);
                producer.send(msg);
            }
            if (!snapshotUtility.usesTransactionLifecycle()) {
                // if we don't use XA transactions, commit after each batch
                session.commit();
            }
        } catch (JMSException e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * Private API. Use {@link SinkProcessors#writeJmsQueueP} or {@link
     * SinkProcessors#writeJmsTopicP} instead
     */
    public static <T> ProcessorMetaSupplier supplier(
            String destinationName,
            boolean exactlyOnce,
            SupplierEx<? extends Connection> newConnectionFn,
            BiFunctionEx<? super Session, T, ? extends Message> messageFn,
            boolean isTopic
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(messageFn, "messageFn");

        return ProcessorMetaSupplier.of(PREFERRED_LOCAL_PARALLELISM,
                ConnectorPermission.jms(destinationName, ACTION_WRITE),
                new Supplier<>(destinationName, exactlyOnce, newConnectionFn, messageFn, isTopic));
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SupplierEx<? extends Connection> newConnectionFn;
        private final String destinationName;
        private final boolean exactlyOnce;
        private final BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn;
        private final boolean isTopic;

        private transient Connection connection;

        private Supplier(
                String destinationName,
                boolean exactlyOnce,
                SupplierEx<? extends Connection> newConnectionFn,
                BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn,
                boolean isTopic
        ) {
            this.destinationName = destinationName;
            this.exactlyOnce = exactlyOnce;
            this.newConnectionFn = newConnectionFn;
            this.messageFn = messageFn;
            this.isTopic = isTopic;
        }

        @Override
        public void init(@Nonnull Context ignored) throws Exception {
            connection = newConnectionFn.get();
            connection.start();
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            SupplierEx<Processor> supplier =
                    () -> new WriteJmsP<>(connection, destinationName, exactlyOnce, messageFn, isTopic);
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void close(Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public List<Permission> permissions() {
            return singletonList(ConnectorPermission.jms(destinationName, ACTION_WRITE));
        }
    }
}
