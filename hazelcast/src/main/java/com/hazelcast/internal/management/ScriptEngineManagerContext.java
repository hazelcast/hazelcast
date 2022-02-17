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

package com.hazelcast.internal.management;

import javax.script.ScriptEngineManager;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Accessor for {@link javax.script.ScriptEngineManager}
 */
public final class ScriptEngineManagerContext {

    private static volatile ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

    //we don't want instances.
    private ScriptEngineManagerContext() {
    }

    public static ScriptEngineManager getScriptEngineManager() {
        return scriptEngineManager;
    }

    public static void setScriptEngineManager(ScriptEngineManager scriptEngineManager) {
        checkNotNull(scriptEngineManager, "ScriptEngineManager is required!");
        ScriptEngineManagerContext.scriptEngineManager = scriptEngineManager;
    }
}
