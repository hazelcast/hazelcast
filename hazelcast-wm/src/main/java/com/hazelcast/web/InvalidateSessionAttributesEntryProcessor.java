/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * Invalidates all attributes for a {@link DestroySessionEntryProcessor destroyed} session, removing them
 * from the clustered map.
 */
public class InvalidateSessionAttributesEntryProcessor extends AbstractWebDataEntryProcessor<Object> {

    private String sessionIdWithAttributeSeparator;

    // Serialization Constructor
    public InvalidateSessionAttributesEntryProcessor() {
    }

    public InvalidateSessionAttributesEntryProcessor(String sessionId) {
            this.sessionIdWithAttributeSeparator = sessionId + WebFilter.HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR;
    }

    @Override
    public int getId() {
        return WebDataSerializerHook.INVALIDATE_SESSION_ATTRIBUTES_ID;
    }

    @Override
    public Object process(Entry<String, Object> entry) {
        Object key = entry.getKey();
        if (key instanceof String) {
            String k = (String) key;
            if (k.startsWith(sessionIdWithAttributeSeparator)) {
                entry.setValue(null);
            }
        }
        return null;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sessionIdWithAttributeSeparator = in.readUTF();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sessionIdWithAttributeSeparator);
    }
}
