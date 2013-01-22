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

package com.hazelcast.topic;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.KeyBasedOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * User: sancar
 * Date: 12/31/12
 * Time: 12:10 PM
 */
public class PublishOperation extends AbstractNamedOperation implements KeyBasedOperation {

    private Data message;

    public PublishOperation() {
        super();
    }

    public PublishOperation(String name, Data message) {
        super(name);
        this.message = message;
    }

    @Override
    public void run() throws Exception {

        TopicEvent topicEvent = new TopicEvent(name, message);
        getNodeEngine().getEventService().publishEvent(TopicService.NAME, getNodeEngine().getEventService().getRegistrations(TopicService.NAME, name), topicEvent);

    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    public int getKeyHash() {
        String key = TopicService.NAME + getName();
        return key.hashCode();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        message.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        message = new Data();
        message.readData(in);
    }
}
