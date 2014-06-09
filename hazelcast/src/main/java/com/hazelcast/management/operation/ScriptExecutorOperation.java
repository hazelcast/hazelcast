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

import com.hazelcast.management.ScriptEngineManagerContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class ScriptExecutorOperation extends Operation {

    private String engineName;
    private String script;
    private Map<String, Object> bindings;
    private Object result;

    public ScriptExecutorOperation() {
    }

    public ScriptExecutorOperation(String engineName, String script, Map<String, Object> bindings) {
        this.engineName = engineName;
        this.script = script;
        this.bindings = bindings;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();
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
        try {
            this.result = engine.eval(script);
        } catch (ScriptException e) {
            this.result = e.getMessage();
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
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

    @Override
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
