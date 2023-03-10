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
import com.hazelcast.jet.JetException;

import java.io.IOException;
import java.lang.reflect.Method;

final class ExecuteJarStrategyHelper {

    private ExecuteJarStrategyHelper() {
    }

    static String findMainClassNameForJar(String mainClassName, String jarPath)
            throws IOException {
        MainClassNameFinder mainClassNameFinder = new MainClassNameFinder();
        mainClassNameFinder.findMainClass(mainClassName, jarPath);

        if (mainClassNameFinder.hasError()) {
            String errorMessage = mainClassNameFinder.getErrorMessage();
            throw new JetException(errorMessage);
        }
        return mainClassNameFinder.getMainClassName();
    }

    static Method findMainMethodForJar(ClassLoader classLoader, String mainClassName) throws ClassNotFoundException {
        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.findMainMethod(classLoader, mainClassName);

        if (mainMethodFinder.hasError()) {
            String errorMessage = mainMethodFinder.getErrorMessage();
            throw new JetException(errorMessage);
        }
        return mainMethodFinder.getMainMethod();
    }

    static BootstrappedJetProxy setupJetProxy(BootstrappedInstanceProxy instanceProxy,
                                              ExecuteJobParameters executeJobParameters) {
        BootstrappedJetProxy bootstrappedJetProxy = instanceProxy.getJet();
        bootstrappedJetProxy.setExecuteJobParameters(executeJobParameters);
        return bootstrappedJetProxy;
    }
}
