/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.codec.JetAddJobStatusListenerCodec;
import com.hazelcast.client.impl.protocol.codec.JetExistsDistributedObjectCodec;
import com.hazelcast.client.impl.protocol.codec.JetExportSnapshotCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSummaryListCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSuspensionCauseCodec;
import com.hazelcast.client.impl.protocol.codec.JetIsJobUserCancelledCodec;
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetRemoveJobStatusListenerCodec;
import com.hazelcast.client.impl.protocol.codec.JetResumeJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetTerminateJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetUpdateJobConfigCodec;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMultipartCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>(50);
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
        factories.put(JetSubmitJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetSubmitJobMessageTask(cm, node, con));
        factories.put(JetTerminateJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetTerminateJobMessageTask(cm, node, con));
        factories.put(JetGetJobStatusCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobStatusMessageTask(cm, node, con));
        factories.put(JetGetJobSuspensionCauseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSuspensionCauseMessageTask(cm, node, con));
        factories.put(JetGetJobMetricsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobMetricsMessageTask(cm, node, con));
        factories.put(JetGetJobIdsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobIdsMessageTask(cm, node, con));
        factories.put(JetJoinSubmittedJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetJoinSubmittedJobMessageTask(cm, node, con));
        factories.put(JetGetJobSubmissionTimeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSubmissionTimeMessageTask(cm, node, con));
        factories.put(JetGetJobConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobConfigMessageTask(cm, node, con));
        factories.put(JetUpdateJobConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUpdateJobConfigMessageTask(cm, node, con));
        factories.put(JetResumeJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetResumeJobMessageTask(cm, node, con));
        factories.put(JetExportSnapshotCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetExportSnapshotMessageTask(cm, node, con));
        factories.put(JetGetJobSummaryListCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSummaryListMessageTask(cm, node, con));
        factories.put(JetGetJobAndSqlSummaryListCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobAndSqlSummaryListMessageTask(cm, node, con));
        factories.put(JetExistsDistributedObjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetExistsDistributedObjectMessageTask(cm, node, con));
        factories.put(JetIsJobUserCancelledCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetIsJobUserCancelledMessageTask(cm, node, con));
        factories.put(JetUploadJobMetaDataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUploadJobMetaDataTask(cm, node, con));
        factories.put(JetUploadJobMultipartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUploadJobMultipartTask(cm, node, con));
        factories.put(JetAddJobStatusListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetAddJobStatusListenerMessageTask(cm, node, con));
        factories.put(JetRemoveJobStatusListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetRemoveJobStatusListenerMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
