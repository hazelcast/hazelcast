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
