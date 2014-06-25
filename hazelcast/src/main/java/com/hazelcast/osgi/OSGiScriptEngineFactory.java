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

package com.hazelcast.osgi;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.List;

/*
Imported from Apache Felix project.
http://svn.apache.org/repos/asf/felix/trunk/mishell/src/main/java/org/apache/felix/mishell/OSGiScriptEngineFactory.java
*/

/**
 * This is a wrapper class for the ScriptEngineFactory class that deals with context class loader issues
 * It is necessary because engines (at least ruby) use the context classloader to find their resources
 * (i.e., their "native" classes)
 */
public class OSGiScriptEngineFactory implements ScriptEngineFactory {
    private ScriptEngineFactory factory;
    private ClassLoader contextClassLoader;

    public OSGiScriptEngineFactory(ScriptEngineFactory factory, ClassLoader contextClassLoader) {
        this.factory = factory;
        this.contextClassLoader = contextClassLoader;
    }

    @Override
    public String getEngineName() {
        return factory.getEngineName();
    }

    @Override
    public String getEngineVersion() {
        return factory.getEngineVersion();
    }

    @Override
    public List<String> getExtensions() {
        return factory.getExtensions();
    }

    @Override
    public String getLanguageName() {
        return factory.getLanguageName();
    }

    @Override
    public String getLanguageVersion() {
        return factory.getLanguageVersion();
    }

    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        return factory.getMethodCallSyntax(obj, m, args);
    }

    @Override
    public List<String> getMimeTypes() {
        return factory.getMimeTypes();
    }

    @Override
    public List<String> getNames() {
        return factory.getNames();
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        return factory.getOutputStatement(toDisplay);
    }

    @Override
    public Object getParameter(String key) {
        return factory.getParameter(key);
    }

    @Override
    public String getProgram(String... statements) {
        return factory.getProgram(statements);
    }

    @Override
    public ScriptEngine getScriptEngine() {
        ScriptEngine engine;
        if (contextClassLoader == null) {
            engine = factory.getScriptEngine();
        } else {
            Thread currentThread = Thread.currentThread();
            ClassLoader old = currentThread.getContextClassLoader();
            currentThread.setContextClassLoader(contextClassLoader);
            engine = factory.getScriptEngine();
            currentThread.setContextClassLoader(old);
        }
        return engine;
    }
}
