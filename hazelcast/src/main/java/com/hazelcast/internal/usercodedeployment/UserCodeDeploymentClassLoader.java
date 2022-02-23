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

package com.hazelcast.internal.usercodedeployment;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public class UserCodeDeploymentClassLoader extends ClassLoader {

    private static final ILogger LOG = Logger.getLogger(UserCodeDeploymentClassLoader.class);

    private UserCodeDeploymentService userCodeDeploymentService;

    public UserCodeDeploymentClassLoader(ClassLoader parent) {
        super(parent);
    }

    public void setUserCodeDeploymentService(UserCodeDeploymentService service) {
        this.userCodeDeploymentService = service;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        Class<?> clazz = null;

        if (userCodeDeploymentService != null) {
            // this looks racy, but it's an optimistic optimization - if the class is already loaded
            // by the user code deployment service then it means the parent classloader failed to load it at some
            // point in time. -> we can use it directly without consulting the parent classloader
            // when the class is not found then we will consult the parent classloader and eventually the classloading
            // service
            clazz = userCodeDeploymentService.findLoadedClass(name);
        }
        if (clazz == null) {
            try {
                return super.loadClass(name, resolve);
            } catch (ClassNotFoundException e) {
                if (userCodeDeploymentService == null) {
                    LOG.finest("User Code Deployment classloader is not initialized yet. ");
                    throw e;
                }
                clazz = userCodeDeploymentService.handleClassNotFoundException(name);
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        }
        return clazz;
    }
}
