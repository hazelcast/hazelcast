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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.TOPIC_TEMPLATE_ID, name = "Topic", ns = "Hazelcast.Client.Protocol.Codec")
public interface TopicCodecTemplate {
    /**
     * Publishes the message to all subscribers of this topic
     *
     * @param name Name of the Topic
     * @param message The message to publish to all subscribers of this topic
     *
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void publish(String name, Data message);

    /**
     * Subscribes to this topic. When someone publishes a message on this topic. onMessage() function of the given
     * MessageListener is called. More than one message listener can be added on one instance.
     *
     * @param name Name of the Topic
     * @param localOnly if true listens only local events on registered member
     * @return returns the registration id
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_TOPIC})
    Object addMessageListener(String name, boolean localOnly);
    /**
     * Stops receiving messages for the given message listener.If the given listener already removed, this method does nothing.
     *
     * @param name Name of the Topic
     * @param registrationId Id of listener registration.
     * @return True if registration is removed, false otherwise
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removeMessageListener(String name, String registrationId);


}
