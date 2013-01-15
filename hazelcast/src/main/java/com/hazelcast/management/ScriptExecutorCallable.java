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

package com.hazelcast.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

public class ScriptExecutorCallable<V> implements DataSerializable, Callable<V>, HazelcastInstanceAware {

    private static final long serialVersionUID = -4729129143589252665L;

    private static final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

    private String engineName;
    private String script;
    private Map<String, Object> bindings;
    private transient HazelcastInstance hazelcast;

    public ScriptExecutorCallable() {
    }

    public ScriptExecutorCallable(String engineName, String script) {
        super();
        this.engineName = engineName;
        this.script = script;
    }

    public ScriptExecutorCallable(String engineName, String script, Map<String, Object> bindings) {
        super();
        this.engineName = engineName;
        this.script = script;
        this.bindings = bindings;
    }

    public V call() throws Exception {
        ScriptEngine engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + engineName + "'.");
        }
        engine.put("hazelcast", hazelcast);
        if (bindings != null) {
            Set<Entry<String, Object>> entries = bindings.entrySet();
            for (Entry<String, Object> entry : entries) {
                engine.put(entry.getKey(), entry.getValue());
            }
        }
        Object result = engine.eval(script);
        if (result == null) {
            return null;
        }
        return (V) result;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(engineName);
        out.writeUTF(script);
        if (bindings != null) {
            out.writeInt(bindings.size());
            Set<Entry<String, Object>> entries = bindings.entrySet();
            for (Entry<String, Object> entry : entries) {
                out.writeUTF(entry.getKey());
                out.writeObject(entry.getValue());
            }
        } else {
            out.writeInt(0);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
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

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    public void setBindings(Map<String, Object> bindings) {
        this.bindings = bindings;
    }
}
