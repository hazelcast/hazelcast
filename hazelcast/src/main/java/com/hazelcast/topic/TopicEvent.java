/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: sancar
 * Date: 12/26/12
 * Time: 3:20 PM
 */
public class TopicEvent implements DataSerializable {

    public String name;

    public Data data;

    public TopicEvent(){
    }

    public TopicEvent(String name, Data data){
        this.name = name;
        this.data = data;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        data = IOUtil.readNullableData(in);
    }
}
