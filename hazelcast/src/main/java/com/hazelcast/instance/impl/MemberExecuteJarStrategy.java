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

package com.hazelcast.instance.impl;

import com.hazelcast.jet.impl.util.ResettableConcurrentMemoizingSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

class MemberExecuteJarStrategy {

    public void executeJar(@Nonnull ResettableConcurrentMemoizingSupplier<BootstrappedInstanceProxy> singleton,
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

            Method main = ExecuteJarStrategyHelper.findMainMethodForJar(classLoader, mainClassName);

            String[] jobArgs = args.toArray(new String[0]);

            // synchronize until the main method is invoked
            synchronized (this) {
                BootstrappedJetProxy bootstrappedJetProxy =
                        ExecuteJarStrategyHelper.setupJetProxy(singleton.remembered(), jarPath, snapshotName, jobName);

                // Clear jobs. We don't need them
                bootstrappedJetProxy.clearSubmittedJobs();

                // upcast args to Object, so it's passed as a single array-typed argument
                main.invoke(null, (Object) jobArgs);
            }
        }
    }
}
