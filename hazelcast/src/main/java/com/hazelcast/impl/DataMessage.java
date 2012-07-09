/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.Message;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializerManager;

import static com.hazelcast.nio.IOUtil.toObject;

public class DataMessage<E> extends Message {

    final Data data;
    private final transient SerializerManager serializerManager;

    public DataMessage(String topicName, Data data, SerializerManager serializerManager) {
        super(topicName, null);
        this.data = data;
        this.serializerManager = serializerManager;
    }

    @Override
    public E getMessageObject() {
        if (serializerManager != null) {
            ThreadContext.get().setCurrentSerializerManager(serializerManager);
        }
        return (E) toObject(data);
    }

    public Data getMessageData() {
        return data;
    }
}
