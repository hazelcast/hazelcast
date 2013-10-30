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
import com.hazelcast.management.operation.ScriptExecutorOperation;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import javax.script.ScriptEngineFactory;

/**
 *
 * Hazelcast OSGi bundle activator
 *
 * initializes script Engines
 *
 */
public class Activator implements BundleActivator {

    private HazelcastInstance hazelcastInstance;

    private ILogger log= Logger.getLogger(Activator.class);


    public void start(BundleContext context) throws Exception {
        final OSGiScriptEngineManager engineManager = new OSGiScriptEngineManager(context);
        ScriptExecutorOperation.setScriptEngineManager(engineManager);

        StringBuffer msg=new StringBuffer("Available script engines are:");
        for (ScriptEngineFactory sef : engineManager.getEngineFactories()) {
            msg.append(sef.getEngineName()+"\n");
        }
        log.info( msg.toString());

        if(System.getProperty("hazelcast.osgi.start") != null){
            hazelcastInstance = Hazelcast.newHazelcastInstance(null);
        }

    }

    public void stop(BundleContext context) throws Exception {
        if(System.getProperty("hazelcast.osgi.start") != null){
            hazelcastInstance.shutdown();
        }
    }

}
