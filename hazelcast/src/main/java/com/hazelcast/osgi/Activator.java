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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.lang.reflect.Method;

/**
 * Hazelcast OSGi bundle activator
 * <p/>
 * initializes script Engines
 */
public class Activator
        implements BundleActivator {

    private static final String HAZELCAST_OSGI_START = "hazelcast.osgi.start";

    private static final ILogger LOGGER = Logger.getLogger(Activator.class);

    private HazelcastInstance hazelcastInstance;

    @Override
    public void start(BundleContext context)
            throws Exception {
        // Try to start javax.scripting - JSR 223
        activateJavaxScripting(context);

        if (System.getProperty(HAZELCAST_OSGI_START) != null) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }
    }

    @Override
    public void stop(BundleContext context)
            throws Exception {
        if (System.getProperty("hazelcast.osgi.start") != null) {
            hazelcastInstance.shutdown();
        }
    }

    private void activateJavaxScripting(BundleContext context)
            throws Exception {

        if (!isJavaxScriptingAvailable()) {
            LOGGER.warning("javax.scripting is not available, scripts from Management Center cannot be executed!");
            return;
        }

        Class<?> clazz = context.getBundle().loadClass("com.hazelcast.osgi.ScriptEngineActivator");
        Method register = clazz.getDeclaredMethod("registerOsgiScriptEngineManager", BundleContext.class);
        register.invoke(clazz, context);
    }

    private boolean isJavaxScriptingAvailable() {
        try {
            Class.forName("javax.script.ScriptEngineManager");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

}
