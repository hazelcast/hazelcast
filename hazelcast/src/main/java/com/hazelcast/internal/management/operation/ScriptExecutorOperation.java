/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.security.AccessControlException;

/**
 * Operation to execute script on the node.
 */
public class ScriptExecutorOperation extends AbstractManagementOperation {

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
        ManagementCenterConfig managementCenterConfig = getNodeEngine().getConfig().getManagementCenterConfig();
        if (!managementCenterConfig.isScriptingEnabled()) {
            throw new AccessControlException("Using ScriptEngine is not allowed on this Hazelcast member.");
        }
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();
        ScriptEngine engine = scriptEngineManager.getEngineByName(engineName);
        if (engine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + engineName + "'.");
        }
        engine.put("hazelcast", getNodeEngine().getHazelcastInstance());
        try {
            this.result = engine.eval(script);
        } catch (ScriptException e) {
            // ScriptException's cause is not serializable - we don't need the cause
            HazelcastException hazelcastException = new HazelcastException(e.getMessage());
            hazelcastException.setStackTrace(e.getStackTrace());
            throw hazelcastException;
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        engineName = in.readUTF();
        script = in.readUTF();
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.SCRIPT_EXECUTOR;
    }
}
