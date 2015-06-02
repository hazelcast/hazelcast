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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CacheBatchInvalidationMessage extends CacheInvalidationMessage {

    private List<CacheSingleInvalidationMessage> invalidationMessages;

    public CacheBatchInvalidationMessage() {

    }

    public CacheBatchInvalidationMessage(String name) {
        super(name);
        this.invalidationMessages = new ArrayList<CacheSingleInvalidationMessage>();
    }

    public CacheBatchInvalidationMessage(String name, int expectedMessageCount) {
        super(name);
        this.invalidationMessages = new ArrayList<CacheSingleInvalidationMessage>(expectedMessageCount);
    }

    public CacheBatchInvalidationMessage(String name,
                                         List<CacheSingleInvalidationMessage> invalidationMessages) {
        super(name);
        assert invalidationMessages != null : "Invalid invalidation messages: " + invalidationMessages;
        this.invalidationMessages = invalidationMessages;
    }

    public CacheBatchInvalidationMessage addInvalidationMessage(CacheSingleInvalidationMessage invalidationMessage) {
        invalidationMessages.add(invalidationMessage);
        return this;
    }

    public List<CacheSingleInvalidationMessage> getInvalidationMessages() {
        return invalidationMessages;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.BATCH_INVALIDATION_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        boolean hasInvalidationMessages = invalidationMessages != null;
        out.writeBoolean(hasInvalidationMessages);
        if (hasInvalidationMessages) {
            out.writeInt(invalidationMessages.size());
            for (CacheSingleInvalidationMessage invalidationMessage : invalidationMessages) {
                out.writeObject(invalidationMessage);
            }
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        if (in.readBoolean()) {
            int size = in.readInt();
            invalidationMessages = new ArrayList<CacheSingleInvalidationMessage>(size);
            for (int i = 0; i < size; i++) {
                invalidationMessages.add((CacheSingleInvalidationMessage) in.readObject());
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheBatchInvalidationMessage{");
        sb.append("name='").append(name).append('\'');
        sb.append(", invalidationMessages=").append(invalidationMessages);
        sb.append('}');
        return sb.toString();
    }

}
