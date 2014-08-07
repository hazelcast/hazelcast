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

package com.hazelcast.topic.impl;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.topic.impl.client.AddMessageListenerRequest;
import com.hazelcast.topic.impl.client.PortableMessage;
import com.hazelcast.topic.impl.client.PublishRequest;
import com.hazelcast.topic.impl.client.RemoveMessageListenerRequest;

import java.util.Collection;

public class TopicPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.TOPIC_PORTABLE_FACTORY, -18);

    public static final int PUBLISH = 1;
    public static final int ADD_LISTENER = 2;
    public static final int REMOVE_LISTENER = 3;
    public static final int PORTABLE_MESSAGE = 4;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case PUBLISH:
                        return new PublishRequest();
                    case ADD_LISTENER:
                        return new AddMessageListenerRequest();
                    case REMOVE_LISTENER:
                        return new RemoveMessageListenerRequest();
                    case PORTABLE_MESSAGE:
                        return new PortableMessage();
                    default:
                        return null;
                }
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
