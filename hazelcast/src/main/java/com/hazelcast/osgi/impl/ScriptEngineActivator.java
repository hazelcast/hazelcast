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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.osgi.framework.BundleContext;

/**
 * This class is used to activate scriptengine managers only if the JSR 223 is available
 */
final class ScriptEngineActivator {

    private static final ILogger LOGGER = Logger.getLogger(ScriptEngineActivator.class);

    private ScriptEngineActivator() {
    }

    /**
     * This method is used to register available script engines in OSGi contexts.<br>
     * <b>Attention:</b> This method is not unused but only called via reflective calls!
     *
     * @param context BundleContext to bind the ScriptEngineManager to
     */
    public static void registerOsgiScriptEngineManager(BundleContext context) {
        OSGiScriptEngineManager scriptEngineManager = new OSGiScriptEngineManager(context);
        ScriptEngineManagerContext.setScriptEngineManager(scriptEngineManager);

        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest(scriptEngineManager.printScriptEngines());
        }
    }

}
