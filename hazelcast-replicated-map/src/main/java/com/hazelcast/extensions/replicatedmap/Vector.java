/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.extensions.replicatedmap;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Vector implements DataSerializable{

    Map<Member, AtomicInteger> map = new ConcurrentHashMap<Member, AtomicInteger>();

    public Vector() {

    }

    public void writeData(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(map.size());
        for(Member member: map.keySet()){
            member.writeData(dataOutput);
            dataOutput.writeInt(map.get(member).get());
        }
    }

    public void readData(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for(int i=0;i<size;i++){
            
            Member m = new MemberImpl();
            m.readData(dataInput);
            int clock = dataInput.readInt();
            map.put(m, new AtomicInteger(clock));
        }
    }

    public boolean desc(Vector o) {
        for(Member m: o.map.keySet()){
            int localClock = (map.get(m)==null? 0:map.get(m).get());
            int remoteClock = (o.map.get(m)) == null? 0:o.map.get(m).get();
            if(localClock < remoteClock){
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Vector{" +
                "map=" + map +
                '}';
    }
}
