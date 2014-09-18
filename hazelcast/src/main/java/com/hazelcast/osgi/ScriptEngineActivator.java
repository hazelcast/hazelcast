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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.management.ScriptEngineManagerContext;
import org.osgi.framework.BundleContext;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

/**
 * This class is used to activate scriptengine managers only if the JSR 223 is available
 */
final class ScriptEngineActivator {

    private static final ILogger LOGGER = Logger.getLogger(ScriptEngineActivator.class);

    private ScriptEngineActivator() {
    }

    /**
     * This method is used to register available script engines in OSGi contexts.<br/>
     * <b>Attention:</b> This method is not unused but only called via reflective calls!
     *
     * @param context BundleContext to bind the ScriptEngineManager to
     */
    public static void registerOsgiScriptEngineManager(BundleContext context) {
        ScriptEngineManager scriptEngineManager = new OSGiScriptEngineManager(context);
        ScriptEngineManagerContext.setScriptEngineManager(scriptEngineManager);

        if (LOGGER.isFinestEnabled()) {
            StringBuilder msg = new StringBuilder("Available script engines are:");
            for (ScriptEngineFactory scriptEngineFactory : scriptEngineManager.getEngineFactories()) {
                msg.append(scriptEngineFactory.getEngineName()).append('\n');
            }
            LOGGER.finest(msg.toString());
        }
    }
}
