/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.topic.TopicOverloadException;

/**
 * Hazelcast provides distribution mechanism for publishing messages that are delivered to multiple subscribers,
 * which is also known as a publish/subscribe (pub/sub) messaging model. Publish and subscriptions are cluster-wide.
 * When a member subscribes for a topic, it is actually registering for messages published by any member in the cluster,
 * including the new members joined after you added the listener.
 * <p/>Messages are ordered, meaning that listeners(subscribers)
 * will process the messages in the order they are actually published. If cluster member M publishes messages
 * m1, m2, m3...mn to a topic T, then Hazelcast makes sure that all of the subscribers of topic T will receive
 * and process m1, m2, m3...mn in order.
 *
 * Since Hazelcast 3.5 it is possible to have reliable topics. Normally all topics rely on the a shared eventing system and
 * shared threads. With Hazelcast 3.5 it is possible to configure a topic to be reliable and to gets its own
 * {@link com.hazelcast.ringbuffer.Ringbuffer} to store events and to gets its own executor to process events. The events
 * in the ringbuffer are replicated, so they won't get lost when a node goes down.
 */
public interface ITopic<E> extends DistributedObject {

    /**
     * Returns the name of this ITopic instance
     *
     * @return name of this ITopic instance
     */
    String getName();

    /**
     * Publishes the message to all subscribers of this topic
     *
     * @param message the message to publish to all subscribers of this topic
     * @throws TopicOverloadException if the consumer is too slow. Only works in combination with
     *                                                   reliable topic.
     */
    void publish(E message);

    /**
     * Subscribes to this topic. When someone publishes a message on this topic.
     * onMessage() function of the given MessageListener is called. More than one message listener can be
     * added on one instance.
     *
     * @param listener the MessageListener to add.
     *
     * @return returns the registration id.
     * @throws java.lang.NullPointerException if listener is null.
     */
    String addMessageListener(MessageListener<E> listener);

    /**
     * Stops receiving messages for the given message listener. If the given listener already removed,
     * this method does nothing.
     *
     * @param registrationId Id of listener registration.
     *
     * @return true if registration is removed, false otherwise
     */
    boolean removeMessageListener(String registrationId);

    /**
     * Returns statistics about this topic, like total number of publishes/receives
     *
     * @return statistics about this topic
     */
    LocalTopicStats getLocalTopicStats();
}
