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

package com.hazelcast.topic;

import com.hazelcast.core.DistributedObject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * Hazelcast provides distribution mechanism for publishing messages that are
 * delivered to multiple subscribers, which is also known as a publish/subscribe
 * (pub/sub) messaging model. Publish and subscriptions are cluster-wide.
 * <p>
 * When a member subscribes for a topic, it is actually registering for messages
 * published by any member in the cluster, including the new members joined after
 * you added the listener.
 * <p>
 * Messages are ordered, meaning that listeners(subscribers) will process the
 * messages in the order they are actually published. If cluster member M
 * publishes messages m1, m2, m3...mn to a topic T, then Hazelcast makes sure
 * that all of the subscribers of topic T will receive and process m1, m2,
 * m3...mn in order.
 * <p>
 * Since Hazelcast 3.5 it is possible to have reliable topics. Normally all
 * topics rely on the shared eventing system and shared threads. With Hazelcast
 * 3.5 it is possible to configure a topic to be reliable and to get its own
 * {@link com.hazelcast.ringbuffer.Ringbuffer} to store events and to get its
 * own executor to process events. The events in the ringbuffer are replicated,
 * so they won't get lost when a node goes down.
 *
 * @param <E> the type of the message
 */
public interface ITopic<E> extends DistributedObject {

    /**
     * Returns the name of this ITopic instance.
     *
     * @return name of this ITopic instance
     */
    String getName();

    /**
     * Publishes the message to all subscribers of this topic.
     *
     * @param message the message to publish to all subscribers of this topic
     * @throws TopicOverloadException if the consumer is too slow
     *                                (only works in combination with reliable topic)
     */
    void publish(@Nonnull E message);

    /**
     * Publishes the message asynchronously to all subscribers of this topic.
     *
     * @param message the message to publish asynchronously to all subscribers of this topic
     * @return the CompletionStage to synchronize on completion.
     */
    CompletionStage<Void> publishAsync(@Nonnull E message);

    /**
     * Subscribes to this topic. When a message is published, the
     * {@link MessageListener#onMessage(Message)} method of the given
     * MessageListener is called.
     * More than one message listener can be added on one instance.
     * See {@link ReliableMessageListener} to better integrate with a reliable topic.
     *
     * @param listener the MessageListener to add
     * @return returns the registration ID
     * @throws java.lang.NullPointerException if listener is {@code null}
     */
    @Nonnull
    UUID addMessageListener(@Nonnull MessageListener<E> listener);

    /**
     * Stops receiving messages for the given message listener.
     * <p>
     * If the given listener already removed, this method does nothing.
     *
     * @param registrationId ID of listener registration
     * @return {@code true} if registration is removed, {@code false} otherwise
     */
    boolean removeMessageListener(@Nonnull UUID registrationId);

    /**
     * Returns statistics about this topic, like total number of publishes/receives.
     * The statistics are local to this member and represent the activity on
     * this member, not the entire cluster.
     *
     * @return statistics about this topic
     */
    @Nonnull LocalTopicStats getLocalTopicStats();

    /**
     * Publishes all messages to all subscribers of this topic.
     *
     * @param messages the messages to publish to all subscribers of this topic
     * @throws TopicOverloadException if the consumer is too slow
     *                                (only works in combination with reliable topic)
     */
    void publishAll(@Nonnull Collection<? extends E> messages) throws ExecutionException, InterruptedException;

    /**
     * Publishes all messages asynchronously to all subscribers of this topic.
     *
     * @param messages the messages to publish asynchronously to all subscribers of this topic
     * @return the CompletionStage to synchronize on completion.
     */
    CompletionStage<Void> publishAllAsync(@Nonnull Collection<? extends E> messages);
}
