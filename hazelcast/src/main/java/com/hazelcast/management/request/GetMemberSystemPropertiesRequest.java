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

package com.hazelcast.management.request;

import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

// author: sancar - 12.12.2012
public class GetMemberSystemPropertiesRequest implements ConsoleRequest {

    public GetMemberSystemPropertiesRequest() {
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_SYSTEM_PROPERTIES;
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        int size = in.readInt();
        String[] temp;
        for (int i = 0; i < size; i++) {
            temp = in.readUTF().split(":#");
            properties.put(temp[0], temp.length == 1 ? "" : temp[1]);
        }
        return properties;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        Properties properties = System.getProperties();
        dos.writeInt(properties.size());

        for (Object property : properties.keySet()) {
            dos.writeUTF((String) property + ":#" + (String) properties.get(property));
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {

    }

    public void readData(ObjectDataInput in) throws IOException {

    }
}
