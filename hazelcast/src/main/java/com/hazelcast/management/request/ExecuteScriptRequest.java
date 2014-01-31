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

import com.hazelcast.core.Member;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.operation.ScriptExecutorOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class ExecuteScriptRequest implements ConsoleRequest {

    private static final byte COLLECTION = 2;
    private String script;
    private String engine;
    private Set<Address> targets;
    private boolean targetAllMembers = false;
    private Map<String, Object> bindings;

    public ExecuteScriptRequest() {
    }

    public ExecuteScriptRequest(String script, String engine,
                                boolean targetAllMembers, Map<String, Object> bindings) {
        this.script = script;
        this.engine = engine;
        this.targets = new HashSet<Address>(0);
        this.targetAllMembers = targetAllMembers;
        this.bindings = bindings;
    }

    public ExecuteScriptRequest(String script, String engine,
                                Set<Address> targets, Map<String, Object> bindings) {
        this.script = script;
        this.targets = targets;
        this.engine = engine;
        this.targetAllMembers = false;
        this.bindings = bindings;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        ArrayList result;
        if (targetAllMembers) {
            final Set<Member> members = mcs.getHazelcastInstance().getCluster().getMembers();
            final ArrayList list = new ArrayList(members.size());
            for (Member member : members) {
                list.add(mcs.callOnMember(member, new ScriptExecutorOperation(engine, script, bindings)));
            }
            result = list;
        } else {
            final ArrayList list = new ArrayList(targets.size());
            for (Address address : targets) {
                list.add(mcs.callOnAddress(address, new ScriptExecutorOperation(engine, script, bindings)));
            }
            result = list;
        }

        dos.writeByte(COLLECTION);//This line left here for compatibility among 3.x
        //TODO: Currently returning complex data structures like map,array are not possible.
        writeCollection(dos, result);
    }

    @Override
    public Object readResponse(ObjectDataInput in) throws IOException {
        byte flag = in.readByte();//This line left here for compatibility among 3.x
        if (flag == COLLECTION) {
            return readCollection(in);
        }
        return null;
    }

    private void writeMap(ObjectDataOutput dos, Map result) throws IOException {
        int size = result != null ? result.size() : 0;
        dos.writeInt(size);
        if (size > 0) {
            Set<Entry<Object, Object>> entries = result.entrySet();
            for (Entry<Object, Object> entry : entries) {
                dos.writeObject(entry.getKey());
                dos.writeObject(entry.getValue());
            }
        }
    }

    private Map readMap(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map props = new HashMap(size);
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                Object key = in.readObject();
                Object value = in.readObject();
                props.put(key, value);
            }
        }
        return props;
    }

    private void writeCollection(ObjectDataOutput dos, Collection result) throws IOException {
        int size = result != null ? result.size() : 0;
        dos.writeInt(size);
        if (size > 0) {
            for (Object aResult : result) {
                dos.writeObject(aResult);
            }
        }
    }

    private Collection readCollection(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Collection coll = new ArrayList(size);
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                Object value = in.readObject();
                coll.add(value);
            }
        }
        return coll;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(script);
        out.writeUTF(engine);
        out.writeBoolean(targetAllMembers);
        out.writeInt(targets.size());
        for (Address target : targets) {
            target.writeData(out);
        }
        writeMap(out, bindings);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        script = in.readUTF();
        engine = in.readUTF();
        targetAllMembers = in.readBoolean();
        int size = in.readInt();
        targets = new HashSet<Address>(size);
        for (int i = 0; i < size; i++) {
            Address target = new Address();
            target.readData(in);
            targets.add(target);
        }
        bindings = readMap(in);
    }
}
