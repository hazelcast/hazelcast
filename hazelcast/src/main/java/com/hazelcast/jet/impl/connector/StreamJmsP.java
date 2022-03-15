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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.JmsSourceBuilder;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;

/**
 * Private API. Access via {@link SourceProcessors#streamJmsQueueP} or {@link
 * SourceProcessors#streamJmsTopicP}
 */
public class StreamJmsP<T> extends AbstractProcessor {

    private static final BroadcastKey<String> SEEN_IDS_KEY = broadcastKey("seen");
    private static final long RESTORED_IDS_TTL = MINUTES.toNanos(1);

    private final Connection connection;
    private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
    private final FunctionEx<? super Message, ? extends T> projectionFn;
    private final FunctionEx<? super Message, ?> messageIdFn;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final ProcessingGuarantee guarantee;
    private final Set<Object> seenIds;
    private Set<Object> restoredIds;
    private long restoredIdsExpiration = Long.MAX_VALUE;

    private Session session;
    private MessageConsumer consumer;
    private Traverser<Object> pendingTraverser = Traversers.empty();
    private boolean snapshotInProgress;
    private Traverser<Entry<BroadcastKey<String>, Set<Object>>> snapshotTraverser;

    StreamJmsP(Connection connection,
               FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
               FunctionEx<? super Message, ?> messageIdFn,
               FunctionEx<? super Message, ? extends T> projectionFn,
               EventTimePolicy<? super T> eventTimePolicy,
               ProcessingGuarantee guarantee
    ) {
        this.connection = connection;
        this.consumerFn = consumerFn;
        this.messageIdFn = messageIdFn;
        this.projectionFn = projectionFn;
        this.guarantee = guarantee;

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
        seenIds = guarantee == EXACTLY_ONCE ? new HashSet<>() : emptySet();
        restoredIds = guarantee == EXACTLY_ONCE ? new HashSet<>() : emptySet();
    }

    @Override
    public boolean isCooperative() {
        // if we commit, the commit() method isn't cooperative
        return guarantee == NONE;
    }

    @Override
    protected void init(@Nonnull Context context) throws JMSException {
        session = connection.createSession(guarantee != NONE, DUPS_OK_ACKNOWLEDGE);
        consumer = consumerFn.apply(session);
    }

    private static long handleJmsTimestamp(Message msg) {
        try {
            // as per `getJMSTimestamp` javadoc, it can return 0 if the timestamp was optimized away
            return msg.getJMSTimestamp() == 0 ? EventTimeMapper.NO_NATIVE_TIME : msg.getJMSTimestamp();
        } catch (JMSException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean complete() {
        if (snapshotInProgress) {
            return false;
        }
        while (emitFromTraverser(pendingTraverser)) {
            try {
                Message t = consumer.receiveNoWait();
                if (t == null) {
                    pendingTraverser = eventTimeMapper.flatMapIdle();
                    break;
                }
                if (guarantee == EXACTLY_ONCE) {
                    // We don't know whether the messages with the restored IDs were acknowledged in the previous
                    // execution or not. They are acknowledged in phase-2 of the snapshot which might not be executed.
                    // If we receive a message with a restored ID, we ignore it. But if we don't receive some ID,
                    // we can never safely throw it out.
                    // In order to avoid storing the restored IDs forever, we set a timeout after which we clear the
                    // collection. We start the timeout after receiving the first message, at which time we know the
                    // broker is working. We assume it will redeliver the messages promptly; if it doesn't, we assume
                    // they were acknowledged in the previous execution or delivered to another processor in this
                    // execution.
                    if (restoredIdsExpiration == Long.MAX_VALUE) {
                        restoredIdsExpiration = System.nanoTime() + RESTORED_IDS_TTL;
                    } else if (!restoredIds.isEmpty() && restoredIdsExpiration <= System.nanoTime()) {
                        restoredIds = emptySet();
                    }
                    Object msgId = messageIdFn.apply(t);
                    if (msgId == null) {
                        throw new JetException("Received a message without an ID. All messages must have an ID, " +
                                "you can specify an extracting function using "
                                + JmsSourceBuilder.class.getSimpleName() + ".messageIdFn()");
                    }
                    seenIds.add(msgId);
                    if (restoredIds.remove(msgId)) {
                        logFine(getLogger(), "Redelivered message dropped: %s", t);
                        continue;
                    }
                }
                T projectedItem = projectionFn.apply(t);
                pendingTraverser = projectedItem != null
                        ? eventTimeMapper.flatMapEvent(projectedItem, 0, handleJmsTimestamp(t))
                        : eventTimeMapper.flatMapIdle();
            } catch (JMSException e) {
                throw sneakyThrow(e);
            }
        }
        return false;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        if (!emitFromTraverser(pendingTraverser)) {
            return false;
        }
        snapshotInProgress = guarantee != NONE;
        if (guarantee != EXACTLY_ONCE) {
            return true;
        }
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.traverseItems(seenIds, restoredIds)
                    .filter(ids -> !ids.isEmpty())
                    .map(ids -> entry(SEEN_IDS_KEY, ids))
                    .onFirstNull(() -> snapshotTraverser = null);
            logFine(getLogger(), "Saved %d seenIds and %d restoredIds to snapshot", seenIds.size(), restoredIds.size());
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        if (guarantee == NONE) {
            return true;
        }
        if (success) {
            try {
                session.commit();
                getLogger().fine("Session committed");
            } catch (JMSException e) {
                throw sneakyThrow(e);
            }
            seenIds.clear();
        } else if (guarantee == EXACTLY_ONCE) {
            // We could tolerate snapshot failures, but if we did, the memory usage will grow without a bound.
            // The `seenIds` and also unacknowledged messages in the session will grow without a limit.
            throw new RestartableException("the snapshot failed");
        }
        snapshotInProgress = false;
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (!SEEN_IDS_KEY.equals(key)) {
            throw new RuntimeException("Unexpected key received from snapshot: " + key);
        }
        // Ignore if not in ex-once mode. The user could cancelAndExportSnapshot() and restart with
        // a lower guarantee.
        // We could restore multiple collections: each processor saves up to two collections and it's restored to
        // all processors because we can't control which processor receives which messages.
        if (guarantee == EXACTLY_ONCE) {
            @SuppressWarnings("unchecked")
            Set<Object> castValue = (Set<Object>) value;
            restoredIds.addAll(castValue);
            logFine(getLogger(), "Restored %d seen IDs from snapshot", castValue.size());
        }
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
    }

    /**
     * Private API. Use {@link SourceProcessors#streamJmsQueueP} or {@link
     * SourceProcessors#streamJmsTopicP} instead.
     */
    public static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String destination;
        private final SupplierEx<? extends Connection> newConnectionFn;
        private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
        private final FunctionEx<? super Message, ?> messageIdFn;
        private final FunctionEx<? super Message, ? extends T> projectionFn;
        private final EventTimePolicy<? super T> eventTimePolicy;
        private ProcessingGuarantee sourceGuarantee;

        private transient Connection connection;

        public Supplier(
                String destination,
                ProcessingGuarantee sourceGuarantee,
                EventTimePolicy<? super T> eventTimePolicy,
                SupplierEx<? extends Connection> newConnectionFn,
                FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
                FunctionEx<? super Message, ?> messageIdFn,
                FunctionEx<? super Message, ? extends T> projectionFn
        ) {
            checkSerializable(newConnectionFn, "newConnectionFn");
            checkSerializable(consumerFn, "consumerFn");
            checkSerializable(messageIdFn, "messageIdFn");
            checkSerializable(projectionFn, "projectionFn");

            this.destination = destination;
            this.newConnectionFn = newConnectionFn;
            this.consumerFn = consumerFn;
            this.messageIdFn = messageIdFn;
            this.projectionFn = projectionFn;
            this.eventTimePolicy = eventTimePolicy;
            this.sourceGuarantee = sourceGuarantee;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            connection = newConnectionFn.get();
            connection.start();
            sourceGuarantee = Util.min(sourceGuarantee, context.processingGuarantee());
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return range(0, count)
                    .mapToObj(i -> new StreamJmsP<>(
                            connection, consumerFn, messageIdFn, projectionFn, eventTimePolicy, sourceGuarantee))
                    .collect(Collectors.toList());
        }

        @Override
        public List<Permission> permissions() {
            return singletonList(ConnectorPermission.jms(destination, ACTION_READ));
        }
    }
}
