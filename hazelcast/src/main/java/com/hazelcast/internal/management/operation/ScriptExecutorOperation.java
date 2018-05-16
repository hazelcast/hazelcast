/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.util.ExceptionUtil;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_10;

/**
 * Operation to execute script on the node.
 */
public class ScriptExecutorOperation extends AbstractManagementOperation implements Versioned {

    private String engineName;
    private String script;
    private Object result;

    @SuppressWarnings("unused")
    public ScriptExecutorOperation() {
    }

    public ScriptExecutorOperation(String engineName, String script) {
        this.engineName = engineName;
        this.script = script;
    }

    @Override
    public void run() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();
        ScriptEngine engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + engineName + "'.");
        }
        engine.put("hazelcast", getNodeEngine().getHazelcastInstance());
        try {
            this.result = engine.eval(script);
        } catch (ScriptException e) {
            throw new HazelcastException(ExceptionUtil.toString(e));
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(engineName);
        out.writeUTF(script);
        if (out.getVersion().isUnknownOrLessThan(V3_10)) {
            out.writeInt(0);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        engineName = in.readUTF();
        script = in.readUTF();
        if (in.getVersion().isUnknownOrLessThan(V3_10)) {
            in.readInt();
        }
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.SCRIPT_EXECUTOR;
    }
}
