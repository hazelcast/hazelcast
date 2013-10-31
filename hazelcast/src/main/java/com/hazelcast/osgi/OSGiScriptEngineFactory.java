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

/**
 * This is a wrapper class for the ScriptEngineFactory class that deals with context class loader issues
 * It is necessary because engines (at least ruby) use the context classloader to find their resources (i.e., their "native" classes)
 *
 */

/*
Imported from Apache Felix project.
http://svn.apache.org/repos/asf/felix/trunk/mishell/src/main/java/org/apache/felix/mishell/OSGiScriptEngineFactory.java
*/
public class OSGiScriptEngineFactory implements ScriptEngineFactory{
    private ScriptEngineFactory factory;
    private ClassLoader contextClassLoader;
    public OSGiScriptEngineFactory (ScriptEngineFactory factory, ClassLoader contextClassLoader){
        this.factory=factory;
        this.contextClassLoader=contextClassLoader;
    }
    public String getEngineName() {
        return factory.getEngineName();
    }
    public String getEngineVersion() {
        return factory.getEngineVersion();
    }
    public List<String> getExtensions() {
        return factory.getExtensions();
    }
    public String getLanguageName() {
        return factory.getLanguageName();
    }
    public String getLanguageVersion() {
        return factory.getLanguageVersion();
    }
    public String getMethodCallSyntax(String obj, String m, String... args) {
        return factory.getMethodCallSyntax(obj, m, args);
    }
    public List<String> getMimeTypes() {
        return factory.getMimeTypes();
    }
    public List<String> getNames() {
        return factory.getNames();
    }
    public String getOutputStatement(String toDisplay) {
        return factory.getOutputStatement(toDisplay);
    }
    public Object getParameter(String key) {
        return factory.getParameter(key);
    }
    public String getProgram(String... statements) {
        return factory.getProgram(statements);
    }
    public ScriptEngine getScriptEngine() {
        ScriptEngine engine=null;
        if(contextClassLoader!=null){
            ClassLoader old=Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(contextClassLoader);
            engine=factory.getScriptEngine();
            Thread.currentThread().setContextClassLoader(old);
        }
        else engine=factory.getScriptEngine();
        return engine;
    }


}
