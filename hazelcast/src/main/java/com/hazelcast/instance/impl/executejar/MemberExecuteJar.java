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

public class MemberExecuteJar {

    private static final ILogger LOGGER = Logger.getLogger(MemberExecuteJar.class);

    /**
     * This method is used by a member to execute a jar in a multithreaded environment
     * <p>
     * The startup of the job is not awaited
     */
    public void executeJar(@Nonnull BootstrappedInstanceProxy instanceProxy,
                           ExecuteJobParameters executeJobParameters,
                           @Nullable String mainClassName,
                           @Nonnull List<String> args
    ) throws IOException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {

        String jarPath = executeJobParameters.getJarPath();
        mainClassName = ExecuteJarHelper.findMainClassNameForJar(jarPath, mainClassName);

        URL jarUrl = new File(jarPath).toURI().toURL();
        try (URLClassLoader classLoader = URLClassLoader.newInstance(
                new URL[]{jarUrl},
                MemberExecuteJar.class.getClassLoader())) {

            Method mainMethod = ExecuteJarHelper.findMainMethodForJar(classLoader, mainClassName);

            LOGGER.info("Found mainClassName :" + mainClassName + " and main method");

            invokeMain(instanceProxy, executeJobParameters, mainMethod, args);
        }
    }

    void invokeMain(BootstrappedInstanceProxy instanceProxy, ExecuteJobParameters executeJobParameters,
                    Method mainMethod, List<String> args)
            throws IllegalAccessException, InvocationTargetException {
        try {
            instanceProxy.setExecuteJobParameters(executeJobParameters);

            String[] jobArgs = args.toArray(new String[0]);

            // upcast args to Object, so it's passed as a single array-typed argument
            mainMethod.invoke(null, (Object) jobArgs);
        } finally {
            instanceProxy.removeExecuteJobParameters();
        }
    }
}
