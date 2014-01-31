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

//todo: is this class uses at all since it doesn't execute any logic.
public class EvictLocalMapRequest implements ConsoleRequest {

    String map;
    int percent;

    public EvictLocalMapRequest() {
    }

    public EvictLocalMapRequest(String map, int percent) {
        this.map = map;
        this.percent = percent;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EVICT_LOCAL_MAP;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        //todo: unused code: does this Request do anything at all?
//        EvictLocalMapEntriesCallable call = new EvictLocalMapEntriesCallable(map, percent);
//        call.setHazelcastInstance(mcs.getHazelcastInstance());
//        mcs.callOnAllMembers(call);
    }

    @Override
    public Object readResponse(ObjectDataInput in) throws IOException {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(map);
        out.writeInt(percent);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        map = in.readUTF();
        percent = in.readInt();
    }
}
