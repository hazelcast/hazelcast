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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.impl.reliable.MessageRunner;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;
import com.hazelcast.version.MemberVersion;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ExceptionUtil.peel;

/**
 * Client implementation of  {@link com.hazelcast.topic.impl.reliable.MessageRunner}
 *
 * @param <E> message type
 */
public class ClientReliableMessageRunner<E> extends MessageRunner<E> {

    ClientReliableMessageRunner(String id, ReliableMessageListener<E> listener,
                                Ringbuffer<ReliableTopicMessage> ringbuffer, String topicName,
                                int batchSze, SerializationService serializationService,
                                Executor executor, ConcurrentMap<String, MessageRunner<E>> runnersMap,
                                ILogger logger) {
        super(id, listener, ringbuffer, topicName, batchSze, serializationService, executor, runnersMap, logger);
    }

    @Override
    protected void updateStatistics() {

    }

    @Override
    protected Member getMember(ReliableTopicMessage m) {
        Member member = null;
        if (m.getPublisherAddress() != null) {
            member = new com.hazelcast.client.impl.MemberImpl(m.getPublisherAddress(), MemberVersion.UNKNOWN);
        }
        return member;
    }

    @Override
    protected boolean handleInternalException(Throwable t) {
        if (t instanceof HazelcastClientOfflineException) {
            if (logger.isFinestEnabled()) {
                logger.finest("MessageListener " + listener + " on topic: " + topicName + " got exception: " + t
                        + ". Continuing from last known sequence: " + sequence);
            }
            return true;
        }
        return super.handleInternalException(t);
    }

    @Override
    protected Throwable adjustThrowable(Throwable t) {
        return peel(t);
    }

    @Override
    protected long getHeadSequence(StaleSequenceException staleSequenceException) {
        return ringbuffer.headSequence();
    }
}
