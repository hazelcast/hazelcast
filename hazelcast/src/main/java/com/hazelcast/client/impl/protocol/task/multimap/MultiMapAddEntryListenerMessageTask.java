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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapMessageType#MULTIMAP_ADDENTRYLISTENER}
 */
public class MultiMapAddEntryListenerMessageTask
        extends AbstractMultiMapAddEntryListenerMessageTask<MultiMapAddEntryListenerCodec.RequestParameters> {

    public MultiMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean shouldIncludeValue() {
        return parameters.includeValue;
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    protected ClientMessage encodeEvent(Data key, Data value, Data oldValue, int type, String uuid, int numberOfEntriesAffected) {
        return MultiMapAddEntryListenerCodec.encodeEntryEvent(key, value, oldValue,
                null, type, uuid, numberOfEntriesAffected);
    }

    @Override
    protected MultiMapAddEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapAddEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapAddEntryListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.includeValue};
    }
}
