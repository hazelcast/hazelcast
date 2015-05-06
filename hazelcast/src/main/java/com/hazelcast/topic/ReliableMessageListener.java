/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MessageListener;
import com.hazelcast.spi.annotation.Beta;

/**
 * A {@link MessageListener} to better integrate with the reliable topic.
 *
 * If a regular MessageListener is registered on a reliable topic, the message listener works fine, but it can't do much
 * more than listening to messages.
 *
 * If a ReliableMessageListener is registered on a normal topic, only the {@link MessageListener} methods will be called.
 *
 * <h1>Durable Subscription</h1>
 * The ReliableMessageListener allows you to control where you want to start processing a message when the listener is
 * registered. This makes it possible to create a durable subscription by storing the sequence of the last message and
 * using this sequenceId as the sequenceId to start from.
 *
 * <h1>Exception handling</h1>
 * The ReliableMessageListener also gives the ability to deal with exception using the {@link #isTerminal(Throwable)}
 * method. If a plain MessageListener is used, then it won't terminate on exceptions and keep on running. But in some
 * cases it is better to stop running.
 *
 * <h1>Global order</h1>
 * The ReliableMessageListener will always get all events in order (global order). It will not get duplicates and
 * there will only gaps if it too slow. For more information see {@link #isLossTolerant()}.
 *
 * <h1>Delivery guarantees</h1>
 * Because the ReliableMessageListener control which item it wants to continue from on restart, it is very easy to provide
 * an at-least-once or at-most-once delivery guarantee. The storeSequence is always called before a message is processed;
 * so it can be persisted on some non volatile storage. When the {@link #retrieveInitialSequence()} returns the stored
 * sequence, then a at-least-once delivery is implemented since the same item is now being processed twice. To implement
 * an at-most-once delivery guarantee, add 1 to stored sequence when the {@link #retrieveInitialSequence()} is called.
 *
 * @param <E>
 */
@Beta
public interface ReliableMessageListener<E> extends MessageListener<E> {

    /**
     * Retrieves the initial sequence this ReliableMessageListener should start from.
     *
     * Return -1 if there is no initial sequence and you want to start from the next published message.
     *
     * If you intent to create a durable subscriber, so continue where you stopped the previous time, load the previous sequence
     * and add 1. If you don't add one, then you will be receiving the same message twice.
     *
     * @return the initial sequence
     */
    long retrieveInitialSequence();

    /**
     * Informs the ReliableMessageListener that it should store the sequence. This method is called before the message is
     * processed. Can be used to make a durable subscription.
     *
     * @param sequence the sequence
     */
    void storeSequence(long sequence);

    /**
     * Checks if this ReliableMessageListener is able to deal with message loss. Even though the reliable topic promises to
     * be reliable, it can be that a MessageListener is too slow. Eventually the message won't be available anymore.
     *
     * If the ReliableMessageListener is not loss tolerant and the topic detects that there are missing messages, it will
     * terminate the ReliableMessageListener.
     *
     * @return true if the ReliableMessageListener is tolerant towards loosing messages.
     */
    boolean isLossTolerant();

    /**
     * Checks if the ReliableMessageListener should be terminated based on an exception thrown while calling
     * {@link #onMessage(com.hazelcast.core.Message)}.
     *
     * @param failure the failure
     * @return true if the ReliableMessageListener should terminate itself, false if it should keep on running.
     */
    boolean isTerminal(Throwable failure);
}
