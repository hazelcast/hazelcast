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
import com.hazelcast.management.operation.ScriptExecutorOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class ExecuteScriptRequest implements ConsoleRequest {

    private static final byte NULL = 0;
    private static final byte MAP = 1;
    private static final byte COLLECTION = 2;
    private static final byte OTHER = -1;

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

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        Object result = null;
        ScriptExecutorOperation operation = new ScriptExecutorOperation(engine, script, bindings);
        if (targetAllMembers) {
            result = mcs.callOnAllMembers(operation);
        } else if (targets.isEmpty()) {
            result = NULL;
        } else if (targets.size() == 1) {
            result = mcs.call(targets.iterator().next(), operation);
        } else {
            result = mcs.callOnAddresses(targets, operation);
        }
        if (result != null) {
            if (result instanceof Map) {
                dos.writeByte(MAP);
                writeMap(dos, (Map) result);
            } else if (result instanceof Collection) {
                dos.writeByte(COLLECTION);
                writeCollection(dos, (Collection) result);
            } else {
                dos.writeByte(OTHER);
                dos.writeObject(result);
            }
        } else {
            dos.writeByte(NULL);
        }
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        byte flag = in.readByte();
        switch (flag) {
            case MAP:
                return readMap(in);
            case COLLECTION:
                return readCollection(in);
            case OTHER:
                return in.readObject();
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
            Iterator iter = result.iterator();
            while (iter.hasNext()) {
                dos.writeObject(iter.next());
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
