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

package com.hazelcast.web;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Map.Entry;

public class InvalidateEntryProcessor extends AbstractEntryProcessor<String, Object> implements DataSerializable {
    private String sessionId;

    // Serialization Constructor
    public InvalidateEntryProcessor() {
        super(true);
    }

    public InvalidateEntryProcessor(String sessionId) {
            this.sessionId = sessionId;
    }

    @Override
    public Object process(Entry<String, Object> entry) {
        Object key = entry.getKey();
        if (key instanceof String) {
            String k = (String) key;
            if (k.startsWith(sessionId + WebFilter.HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR)) {
                entry.setValue(null);
            }
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sessionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sessionId = in.readUTF();
    }
}
