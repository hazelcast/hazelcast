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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastOSGiScriptEngineFactoryTest extends HazelcastOSGiScriptingTest {

    @Test
    public void scriptEngineFactoriesIteratedAndAccessedSuccessfully() {
        OSGiScriptEngineManager scriptEngineManager =
                (OSGiScriptEngineManager) ScriptEngineManagerContext.getScriptEngineManager();

        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            assertTrue(engineFactory instanceof OSGiScriptEngineFactory);
        }

        scriptEngineManager.reloadManagers();

        engineFactories = scriptEngineManager.getEngineFactories();
        assertNotNull(engineFactories);
        for (ScriptEngineFactory engineFactory : engineFactories) {
            assertTrue(engineFactory instanceof OSGiScriptEngineFactory);
        }
    }

    private void verifyScriptEngineFactory(ScriptEngineFactory scriptEngineFactory) {
        assertNotNull(scriptEngineFactory);

        assertEquals("Groovy Scripting Engine", scriptEngineFactory.getEngineName());
        assertNotNull(scriptEngineFactory.getEngineVersion());
        assertArrayEquals(singletonList("groovy").toArray(), scriptEngineFactory.getExtensions().toArray());
        assertEquals("Groovy", scriptEngineFactory.getLanguageName());
        assertNotNull(scriptEngineFactory.getLanguageVersion());
        assertEquals("\"hazelcast\".charAt(0)", scriptEngineFactory.getMethodCallSyntax("\"hazelcast\"", "charAt", "0"));
        assertArrayEquals(singletonList("application/x-groovy").toArray(), scriptEngineFactory.getMimeTypes().toArray());
        assertArrayEquals(asList("groovy", "Groovy").toArray(), scriptEngineFactory.getNames().toArray());
        assertEquals("println(\"hello world\")", scriptEngineFactory.getOutputStatement("hello world"));
        assertEquals("groovy", scriptEngineFactory.getParameter(ScriptEngine.NAME));
        assertEquals("x=1\ny=2\n", scriptEngineFactory.getProgram("x=1", "y=2"));
        assertNotNull(scriptEngineFactory.getScriptEngine());
    }

    @Test
    public void registerAndGetScriptEngineByNameSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        GroovyScriptEngineFactory groovyScriptEngineFactory = new GroovyScriptEngineFactory();
        scriptEngineManager.registerEngineName("groovy", groovyScriptEngineFactory);

        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("groovy");
        assertNotNull(scriptEngine);

        ScriptEngineFactory scriptEngineFactory = scriptEngine.getFactory();
        verifyScriptEngineFactory(scriptEngineFactory);
    }

    @Test
    public void registerAndGetScriptEngineByMimeTypeSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        GroovyScriptEngineFactory groovyScriptEngineFactory = new GroovyScriptEngineFactory();
        scriptEngineManager.registerEngineMimeType("application/x-groovy", groovyScriptEngineFactory);

        ScriptEngine scriptEngine = scriptEngineManager.getEngineByMimeType("application/x-groovy");
        assertNotNull(scriptEngine);

        ScriptEngineFactory scriptEngineFactory = scriptEngine.getFactory();
        verifyScriptEngineFactory(scriptEngineFactory);
    }

    @Test
    public void registerAndGetScriptEngineByExtensionSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        GroovyScriptEngineFactory groovyScriptEngineFactory = new GroovyScriptEngineFactory();
        scriptEngineManager.registerEngineExtension("groovy", groovyScriptEngineFactory);

        ScriptEngine scriptEngine = scriptEngineManager.getEngineByExtension("groovy");
        assertNotNull(scriptEngine);

        ScriptEngineFactory scriptEngineFactory = scriptEngine.getFactory();
        verifyScriptEngineFactory(scriptEngineFactory);
    }
}
