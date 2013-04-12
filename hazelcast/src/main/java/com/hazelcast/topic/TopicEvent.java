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

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 3:20 PM
 */
public class TopicEvent implements DataSerializable {

    public String name;
    public long publishTime;
    public Member publishingMember;
    public Data data;

    public TopicEvent() {
    }

    public TopicEvent(String name, Data data, Member publishingMember) {
        publishTime = Clock.currentTimeMillis();
        this.publishingMember = publishingMember;
        this.name = name;
        this.data = data;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(publishTime);
        out.writeObject(publishingMember);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        publishTime = in.readLong();
        publishingMember = in.readObject();
        data = IOUtil.readNullableData(in);
    }
}
