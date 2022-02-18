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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.DefaultMessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.NoSuchMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;

public class CompositeMessageTaskFactory implements MessageTaskFactory {
    private static final String FACTORY_ID = "com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider";

    private final Node node;
    private final NodeEngine nodeEngine;
    private final Map<Integer, MessageTaskFactory> factories;

    public CompositeMessageTaskFactory(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        MessageTaskFactoryProvider defaultProvider = new DefaultMessageTaskFactoryProvider(this.nodeEngine);
        this.factories = new Int2ObjectHashMap<>(defaultProvider.getFactories().size());
        loadProvider(defaultProvider);
        loadServices();
    }

    private void loadProvider(MessageTaskFactoryProvider provider) {
        Int2ObjectHashMap<MessageTaskFactory> providerFactories = provider.getFactories();
        this.factories.putAll(providerFactories);
    }

    private void loadServices() {
        try {
            ClassLoader classLoader = this.node.getConfigClassLoader();
            Iterator<Class<MessageTaskFactoryProvider>> iter = ServiceLoader.classIterator(
                    MessageTaskFactoryProvider.class, FACTORY_ID, classLoader);

            while (iter.hasNext()) {
                Class<MessageTaskFactoryProvider> clazz = iter.next();
                Constructor<MessageTaskFactoryProvider> constructor = clazz
                        .getDeclaredConstructor(new Class[]{NodeEngine.class});
                MessageTaskFactoryProvider messageTaskProvider = constructor.newInstance(this.nodeEngine);
                loadProvider(messageTaskProvider);
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public MessageTask create(ClientMessage clientMessage, Connection connection) {
        try {
            final MessageTaskFactory factory = this.factories.get(clientMessage.getMessageType());
            if (factory != null) {
                return factory.create(clientMessage, connection);
            }
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return new NoSuchMessageTask(clientMessage, this.node, connection);
    }
}
