/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.instance.impl.BootstrappedInstanceProxy;
import com.hazelcast.instance.impl.BootstrappedJetProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class MemberExecuteJarStrategy {

    private static final ILogger LOGGER = Logger.getLogger(MemberExecuteJarStrategy.class.getName());

    /**
     * This method is used by a member to execute a jar in a multithreaded environment
     * <p>
     * It is suggested that the jar start a single job. Because all the specified parameters are applied to each job
     * For example if the job name is specified then the jar can not submit multiple jobs, since job names should be
     * unique across the cluster
     * <p>
     * The startup of the jobs are not awaited
     */
    public void executeJar(@Nonnull BootstrappedInstanceProxy instanceProxy,
                           @Nonnull String jarPath,
                           @Nullable String snapshotName,
                           @Nullable String jobName,
                           @Nullable String mainClassName,
                           @Nonnull List<String> args
    ) throws IOException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {

        mainClassName = ExecuteJarStrategyHelper.findMainClassNameForJar(mainClassName, jarPath);

        URL jarUrl = new File(jarPath).toURI().toURL();
        try (URLClassLoader classLoader = URLClassLoader.newInstance(
                new URL[]{jarUrl},
                MemberExecuteJarStrategy.class.getClassLoader())) {

            Method mainMethod = ExecuteJarStrategyHelper.findMainMethodForJar(classLoader, mainClassName);

            LOGGER.info("Found mainClassName :" + mainClassName + " and main method");

            String[] jobArgs = args.toArray(new String[0]);

            invokeMain(instanceProxy, jarPath, snapshotName, jobName, mainMethod, jobArgs);
        }
    }

    // synchronized until main method finishes
    synchronized void invokeMain(BootstrappedInstanceProxy instanceProxy, String jarPath, String snapshotName,
                                 String jobName, Method main, String[] jobArgs)
            throws IllegalAccessException, InvocationTargetException {

        BootstrappedJetProxy bootstrappedJetProxy =
                ExecuteJarStrategyHelper.setupJetProxy(instanceProxy, jarPath, snapshotName, jobName);

        // Clear jobs. We don't need them
        bootstrappedJetProxy.clearSubmittedJobs();

        // upcast args to Object, so it's passed as a single array-typed argument
        main.invoke(null, (Object) jobArgs);
    }
}
