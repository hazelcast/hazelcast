/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsByNameCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.JetResumeJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetTerminateJobCodec;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.metrics.mancenter.JetReadMetricsMessageTask;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
        factories[JetSubmitJobCodec.RequestParameters.TYPE.id()] = toFactory(JetSubmitJobMessageTask::new);
        factories[JetTerminateJobCodec.RequestParameters.TYPE.id()] = toFactory(JetTerminateJobMessageTask::new);
        factories[JetGetJobStatusCodec.RequestParameters.TYPE.id()] = toFactory(JetGetJobStatusMessageTask::new);
        factories[JetGetJobIdsCodec.RequestParameters.TYPE.id()] = toFactory(JetGetJobIdsMessageTask::new);
        factories[JetJoinSubmittedJobCodec.RequestParameters.TYPE.id()] = toFactory(JetJoinSubmittedJobMessageTask::new);
        factories[JetGetJobIdsByNameCodec.RequestParameters.TYPE.id()] = toFactory(JetGetJobIdsByNameMessageTask::new);
        factories[JetGetJobSubmissionTimeCodec.RequestParameters.TYPE.id()] =
                toFactory(JetGetJobSubmissionTimeMessageTask::new);
        factories[JetGetJobConfigCodec.REQUEST_TYPE.id()] = toFactory(JetGetJobConfigMessageTask::new);
        factories[JetReadMetricsCodec.REQUEST_TYPE.id()] = toFactory(JetReadMetricsMessageTask::new);
        factories[JetResumeJobCodec.REQUEST_TYPE.id()] = toFactory(JetResumeJobMessageTask::new);
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        return factories.clone();
    }

    private MessageTaskFactory toFactory(MessageTaskConstructor constructor) {
        return ((clientMessage, connection) -> constructor.construct(clientMessage, node, connection));
    }

    private interface MessageTaskConstructor {
        MessageTask construct(ClientMessage message, Node node, Connection connection);
    }
}

