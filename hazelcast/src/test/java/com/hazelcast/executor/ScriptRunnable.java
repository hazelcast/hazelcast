/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.util.StringUtil;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.Map;

class ScriptRunnable implements Runnable, Serializable, HazelcastInstanceAware {

    private final String script;
    private final Map<String, ?> map;
    private transient HazelcastInstance hazelcastInstance;

    ScriptRunnable(String script, Map<String, ?> map) {
        this.script = script;
        this.map = map;
    }

    @Override
    public void run() {
        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        ScriptEngine e = scriptEngineManager.getEngineByName("javascript");
        if (map != null) {
            for (Map.Entry<String, ?> entry : map.entrySet()) {
                e.put(entry.getKey(), entry.getValue());
            }
        }
        e.put("hazelcast", hazelcastInstance);
        try {
            // for new JavaScript engine called Nashorn we need the compatibility script
            if (e.getFactory().getEngineName().toLowerCase(StringUtil.LOCALE_INTERNAL).contains("nashorn")) {
                e.eval("load('nashorn:mozilla_compat.js');");
            }

            e.eval("importPackage(java.lang);");
            e.eval("importPackage(java.util);");
            e.eval("importPackage(com.hazelcast.core);");
            e.eval("importPackage(com.hazelcast.config);");
            e.eval("importPackage(java.util.concurrent);");
            e.eval("importPackage(org.junit);");
            e.eval(script);
        } catch (ScriptException e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
