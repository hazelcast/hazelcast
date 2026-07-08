/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.protocol.task;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddVectorCollectionConfigCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionClearCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionGetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSizeCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.internal.impl.protocol.task.dynamicconfig.AddVectorCollectionConfigMessageTask;

public class VectorMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private static final int INITIAL_CAPACITY = 10;
    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>(INITIAL_CAPACITY);

    private final Node node;

    public VectorMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = nodeEngine.getNode();
        initFactories();
    }

    public void initFactories() {
        factories.put(DynamicConfigAddVectorCollectionConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddVectorCollectionConfigMessageTask(cm, node, con));
        factories.put(VectorCollectionGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionGetMessageTask(cm, node, con));
        factories.put(VectorCollectionPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionPutMessageTask(cm, node, con));
        factories.put(VectorCollectionPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionPutIfAbsentMessageTask(cm, node, con));
        factories.put(VectorCollectionPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionPutAllMessageTask(cm, node, con));
        factories.put(VectorCollectionSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionSetMessageTask(cm, node, con));
        factories.put(VectorCollectionDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionDeleteMessageTask(cm, node, con));
        factories.put(VectorCollectionRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionRemoveMessageTask(cm, node, con));
        factories.put(VectorCollectionSearchNearVectorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionSearchNearVectorMessageTask(cm, node, con));
        factories.put(VectorCollectionOptimizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionOptimizeMessageTask(cm, node, con));
        factories.put(VectorCollectionClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionClearMessageTask(cm, node, con));
        factories.put(VectorCollectionSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new VectorCollectionSizeMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
