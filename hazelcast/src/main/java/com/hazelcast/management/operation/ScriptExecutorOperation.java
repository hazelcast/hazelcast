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

package com.hazelcast.management.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * User: sancar
 * Date: 3/27/13
 * Time: 10:57 AM
 */
public class ScriptExecutorOperation extends Operation {


    private static final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
    private String engineName;
    private String script;
    private Map<String, Object> bindings;
    private Object result;

    public ScriptExecutorOperation() {
        super();
    }

    public ScriptExecutorOperation(String engineName, String script, Map<String, Object> bindings) {
        super();
        this.engineName = engineName;
        this.script = script;
        this.bindings = bindings;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        ScriptEngine engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + engineName + "'.");
        }
        engine.put("hazelcast", getNodeEngine().getHazelcastInstance());
        if (bindings != null) {
            Set<Map.Entry<String, Object>> entries = bindings.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                engine.put(entry.getKey(), entry.getValue());
            }
        }
        Object result = engine.eval(script);
        this.result = result;
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return true;
    }

    public Object getResponse() {
        return result;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(engineName);
        out.writeUTF(script);
        if (bindings != null) {
            out.writeInt(bindings.size());
            Set<Map.Entry<String, Object>> entries = bindings.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                out.writeUTF(entry.getKey());
                out.writeObject(entry.getValue());
            }
        } else {
            out.writeInt(0);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        engineName = in.readUTF();
        script = in.readUTF();
        int size = in.readInt();
        if (size > 0) {
            bindings = new HashMap<String, Object>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                Object value = in.readObject();
                bindings.put(key, value);
            }
        }
    }
}
