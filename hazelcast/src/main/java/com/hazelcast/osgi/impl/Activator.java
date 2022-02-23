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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.osgi.HazelcastOSGiService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.lang.reflect.Method;

/**
 * Hazelcast OSGi bundle activator.
 *
 * @see org.osgi.framework.BundleActivator
 */
public class Activator
        implements BundleActivator {

    private static final ILogger LOGGER = Logger.getLogger(Activator.class);

    // Defined as `volatile` since Javadoc of `BundleActivator` says that
    // `The Framework must not concurrently call a BundleActivator object`
    // and also it is marked as `@NotThreadSafe`.
    // So it can be called from different threads but cannot be called concurrently.
    private volatile HazelcastInternalOSGiService hazelcastOSGiService;

    @Override
    public void start(BundleContext context)
            throws Exception {
        // Try to start javax.scripting - JSR 223
        activateJavaxScripting(context);

        assert hazelcastOSGiService == null : "Hazelcast OSGI service should be null while starting!";

        // Create a new Hazelcast OSGI service with given bundle and its context
        hazelcastOSGiService = new HazelcastOSGiServiceImpl(context.getBundle());

        // Activate new created Hazelcast OSGI service
        hazelcastOSGiService.activate();
    }

    @Override
    public void stop(BundleContext context)
            throws Exception {
        assert hazelcastOSGiService != null : "Hazelcast OSGI service should not be null while stopping!";

        hazelcastOSGiService.deactivate();
        hazelcastOSGiService = null;
    }

    private void activateJavaxScripting(BundleContext context)
            throws Exception {
        if (isJavaxScriptingAvailable()) {
            Class<?> clazz = context.getBundle().loadClass("com.hazelcast.osgi.impl.ScriptEngineActivator");
            Method register = clazz.getDeclaredMethod("registerOsgiScriptEngineManager", BundleContext.class);
            register.setAccessible(true);
            register.invoke(clazz, context);
        } else {
            LOGGER.warning("javax.scripting is not available, scripts from Management Center cannot be executed!");
        }
    }

    static boolean isJavaxScriptingAvailable() {
        if (Boolean.getBoolean(HazelcastOSGiService.HAZELCAST_OSGI_JSR223_DISABLED)) {
            return false;
        }
        try {
            Class.forName("javax.script.ScriptEngineManager");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

}
