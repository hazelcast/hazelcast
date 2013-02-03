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

package com.hazelcast.impl.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class ScriptExecutorCallable<V> implements DataSerializable, Callable<V>, HazelcastInstanceAware {

    private static final long serialVersionUID = -4729129143589252665L;

    private static final ILogger logger = Logger.getLogger(ScriptExecutorCallable.class.getName());
    private static final String SCRIPT_ENGINE_MANAGER_CLASS = "javax.script.ScriptEngineManager";
    private static final Object/*ScriptEngineManager*/ scriptEngineManager;
    private static final Method/*ScriptEngineManager.getEngineByName*/ mGetEngineByName;
    private static final Exception scriptEngineLoadError;

    static {
        Object manager = null;
        Method method = null;
        Exception error = null;
        try {
            final Class scriptEngineManagerClass = Class.forName(SCRIPT_ENGINE_MANAGER_CLASS);
            manager = scriptEngineManagerClass.newInstance();
            method = scriptEngineManagerClass.getMethod("getEngineByName", new Class[]{String.class});
        } catch (Exception e) {
            error = e;
            logger.log(Level.WARNING, "ScriptEngineManager could not be loaded!", e);
        }
        scriptEngineManager = manager;
        mGetEngineByName = method;
        scriptEngineLoadError = error;
    }

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
        if (scriptEngineLoadError != null) {
            throw new ExecutionException("ScriptEngineManager could not be loaded!", scriptEngineLoadError);
        }
        // ScriptEngine engine = ScriptEngineManager.getEngineByName(engineName);
        Object/*ScriptEngine*/ engine = mGetEngineByName.invoke(scriptEngineManager, engineName);
        if (engine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + engineName + "'.");
        }
        Method put = engine.getClass().getMethod("put", new Class[]{String.class, Object.class});
        put.invoke(engine, "hazelcast", hazelcast);
        if (bindings != null) {
            Set<Entry<String, Object>> entries = bindings.entrySet();
            for (Entry<String, Object> entry : entries) {
                // ScriptEngine.put(key, value);
                put.invoke(engine, entry.getKey(), entry.getValue());
            }
        }
        Method eval = engine.getClass().getMethod("eval", new Class[]{String.class});
        // Object result = ScriptEngine.eval(script);
        Object result = eval.invoke(engine, script);
        if (result == null) {
            return null;
        }
        return (V) result;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(engineName);
        out.writeUTF(script);
        if (bindings != null) {
            out.writeInt(bindings.size());
            Set<Entry<String, Object>> entries = bindings.entrySet();
            for (Entry<String, Object> entry : entries) {
                out.writeUTF(entry.getKey());
                SerializationHelper.writeObject(out, entry.getValue());
            }
        } else {
            out.writeInt(0);
        }
    }

    public void readData(DataInput in) throws IOException {
        engineName = in.readUTF();
        script = in.readUTF();
        int size = in.readInt();
        if (size > 0) {
            bindings = new HashMap<String, Object>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                Object value = SerializationHelper.readObject(in);
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
