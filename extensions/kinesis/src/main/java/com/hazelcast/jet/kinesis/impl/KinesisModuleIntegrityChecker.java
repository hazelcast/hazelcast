/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.ModuleIntegrityChecker;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.jet.kinesis.impl.source.KinesisDataSerializerHook;

public class KinesisModuleIntegrityChecker implements ModuleIntegrityChecker {
    @Override
    public void check() {
        boolean isValid = false;
        try {
            final KinesisDataSerializerHook serializerHook = ServiceLoader.load(
                    KinesisDataSerializerHook.class,
                    "com.hazelcast.DataSerializerHook",
                    this.getClass().getClassLoader()
            );

            isValid = serializerHook != null;
        } catch (Exception ignored) { }

        if (!isValid) {
            throw new HazelcastException("Failed to verify \"KinesisDataSerializerHook\" module integrity, " +
                    "unable to load KinesisDataSerializerHook, please verify that your build system "
                    + "is preserving per-module META-INF/service files in resources");
        }
    }
}
