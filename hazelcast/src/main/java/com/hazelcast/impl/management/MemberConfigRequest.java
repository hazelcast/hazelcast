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

package com.hazelcast.impl.management;

import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// author: sancar - 11.12.2012
public class MemberConfigRequest implements ConsoleRequest{

    public MemberConfigRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_CONFIG;
    }

    public Object readResponse(DataInput in) throws IOException {
        return in.readUTF();
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        String clusterXml;
        clusterXml = new ConfigXmlGenerator(true).generate(mcs.getHazelcastInstance().getConfig());
        dos.writeUTF(clusterXml);
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
    }
}
