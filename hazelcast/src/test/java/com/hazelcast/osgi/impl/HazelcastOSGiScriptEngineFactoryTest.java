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
import java.util.Arrays;
import java.util.List;

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
        assertArrayEquals(Arrays.asList("groovy").toArray(), scriptEngineFactory.getExtensions().toArray());
        assertEquals("Groovy", scriptEngineFactory.getLanguageName());
        assertNotNull(scriptEngineFactory.getLanguageVersion());
        assertEquals("\"hazelcast\".charAt(0)", scriptEngineFactory.getMethodCallSyntax("\"hazelcast\"", "charAt", "0"));
        assertArrayEquals(Arrays.asList("application/x-groovy").toArray(), scriptEngineFactory.getMimeTypes().toArray());
        assertArrayEquals(Arrays.asList("groovy", "Groovy").toArray(), scriptEngineFactory.getNames().toArray());
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
