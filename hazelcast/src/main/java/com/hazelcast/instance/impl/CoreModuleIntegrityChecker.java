/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.util.ServiceLoader;

public class CoreModuleIntegrityChecker implements ModuleIntegrityChecker {
    @Override
    public void check() {
        boolean isValid = false;
        try {
            final ClusterDataSerializerHook serializerHook = ServiceLoader.load(
                    ClusterDataSerializerHook.class,
                    "com.hazelcast.DataSerializerHook",
                    this.getClass().getClassLoader()
            );

            isValid = serializerHook != null;
        } catch (Exception ignored) { }

        if (!isValid) {
            throw new HazelcastException("Failed to verify \"hazelcast\" module integrity, unable to load"
                    + "ClusterDataSerializerHook, please verify that your build system "
                    + "is preserving per-module META-INF/service files in resources");
        }
    }
}
