package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

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
            // For new JavaScript engine called Nashorn we need the compatibility script
            if (e.getFactory().getEngineName().toLowerCase().contains("nashorn")) {
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
