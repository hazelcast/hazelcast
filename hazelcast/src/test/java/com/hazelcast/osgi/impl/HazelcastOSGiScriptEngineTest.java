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

package com.hazelcast.osgi.impl;

import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.codehaus.groovy.jsr223.GroovyScriptEngineFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.StringReader;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastOSGiScriptEngineTest extends HazelcastOSGiScriptingTest {

    @Test
    public void bindingsGetAndSetSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            verifyThatBindingsGetAndSetSuccessfully(engineFactory.getScriptEngine());
        }
    }

    private void verifyThatBindingsGetAndSetSuccessfully(ScriptEngine scriptEngine) {
        assertNotNull(scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE));

        Bindings bindings = scriptEngine.createBindings();
        assertNotEquals(bindings, scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE));

        scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        assertEquals(bindings, scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE));
    }

    @Test
    public void putAndGetOverBindingsSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            verifyThatBindingsPutAndGetOverBindingsSuccessfully(engineFactory.getScriptEngine());
        }
    }

    private void verifyThatBindingsPutAndGetOverBindingsSuccessfully(ScriptEngine scriptEngine) {
        Bindings bindings = scriptEngine.createBindings();
        assertNull(bindings.get("my-key"));
        bindings.put("my-key", "my-value");
        assertEquals("my-value", bindings.get("my-key"));
    }

    @Test
    public void putAndGetSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            verifyThatPutAndGetSuccessfully(engineFactory.getScriptEngine());
        }
    }

    private void verifyThatPutAndGetSuccessfully(ScriptEngine scriptEngine) {
        assertNull(scriptEngine.get("my-key"));
        scriptEngine.put("my-key", "my-value");
        assertEquals("my-value", scriptEngine.get("my-key"));
    }

    @Test
    public void putAndGetContextSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            verifyThatPutAndGetContextSuccessfully(engineFactory.getScriptEngine());
        }
    }

    private void verifyThatPutAndGetContextSuccessfully(ScriptEngine scriptEngine) {
        assertNotNull(scriptEngine.getContext());
        ScriptContext mockScriptContext = mock(ScriptContext.class);
        scriptEngine.setContext(mockScriptContext);
        assertEquals(mockScriptContext, scriptEngine.getContext());
    }

    @Test
    public void scriptEngineEvaluatedSuccessfully() throws ScriptException {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        GroovyScriptEngineFactory groovyScriptEngineFactory = new GroovyScriptEngineFactory();
        scriptEngineManager.registerEngineName("groovy", groovyScriptEngineFactory);

        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("groovy");
        assertNotNull(scriptEngine);

        verifyScriptEngineEvaluation(scriptEngine);
    }

    private void verifyScriptEngineEvaluation(ScriptEngine scriptEngine) throws ScriptException {
        assertNotNull(scriptEngine.getFactory());

        String SCRIPT_SOURCE_WITH_VARIABLE = "\"I am \" + name";
        String SCRIPT_SOURCE_WITHOUT_VARIABLE = "\"I am human\"";

        Bindings bindings = scriptEngine.createBindings();
        bindings.put("name", "Serkan");

        ScriptContext scriptContext = scriptEngine.getContext();
        scriptContext.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        assertEquals("I am Serkan", scriptEngine.eval(SCRIPT_SOURCE_WITH_VARIABLE, bindings));
        assertEquals("I am Serkan", scriptEngine.eval(SCRIPT_SOURCE_WITH_VARIABLE, scriptContext));
        assertEquals("I am human", scriptEngine.eval(SCRIPT_SOURCE_WITHOUT_VARIABLE));

        assertEquals("I am Serkan", scriptEngine.eval(new StringReader(SCRIPT_SOURCE_WITH_VARIABLE), bindings));
        assertEquals("I am Serkan", scriptEngine.eval(new StringReader(SCRIPT_SOURCE_WITH_VARIABLE), scriptContext));
        assertEquals("I am human", scriptEngine.eval(new StringReader(SCRIPT_SOURCE_WITHOUT_VARIABLE)));
    }
}
