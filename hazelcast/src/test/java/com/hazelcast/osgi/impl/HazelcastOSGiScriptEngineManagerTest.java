package com.hazelcast.osgi.impl;

import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.script.Bindings;
import javax.script.ScriptEngineManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastOSGiScriptEngineManagerTest extends HazelcastOSGiScriptingTest {

    @Test
    public void scriptEngineManagerSetSuccessfully() {
        assertNotNull(ScriptEngineManagerContext.getScriptEngineManager());
    }

    @Test
    public void scriptEnginesPrintedSuccessfully() {
        OSGiScriptEngineManager scriptEngineManager =
            (OSGiScriptEngineManager) ScriptEngineManagerContext.getScriptEngineManager();

        assertNotNull(scriptEngineManager.printScriptEngines());
    }

    @Test
    public void bindingsGetAndSetSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        assertNotNull(scriptEngineManager.getBindings());

        Bindings mockBindings = mock(Bindings.class);
        scriptEngineManager.setBindings(mockBindings);
        assertEquals(mockBindings, scriptEngineManager.getBindings());
    }

    @Test
    public void putAndGetOverBindingsSuccessfully() {
        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();

        Bindings bindings = scriptEngineManager.getBindings();

        assertNull(bindings.get("my-key"));
        bindings.put("my-key", "my-value");
        assertEquals("my-value", bindings.get("my-key"));
    }

}
