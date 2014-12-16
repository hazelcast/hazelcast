/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.EventObject;

/**
 * Message for {@link ITopic}.
 *
 * @param <E> message type
 */
public class Message<E> extends EventObject {

    protected E messageObject;
    private final long publishTime;
    private final Member publishingMember;

    public Message(String topicName, E messageObject, long publishTime, Member publishingMember) {
        super(topicName);
        this.messageObject = messageObject;
        this.publishTime = publishTime;
        this.publishingMember = publishingMember;
    }

    /**
     * Returns the published message
     *
     * @return the published message object
     */
    public E getMessageObject() {
        return messageObject;
    }

    /**
     * Return the time when the message is published
     *
     * @return the time when the message is published
     */
    public long getPublishTime() {
        return publishTime;
    }

    /**
     * Returns the member that published the message
     *
     * @return the member that published the message
     */
    public Member getPublishingMember() {
        return publishingMember;
    }
}
