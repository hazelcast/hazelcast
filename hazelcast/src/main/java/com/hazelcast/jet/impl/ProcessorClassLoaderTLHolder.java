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

package com.hazelcast.jet.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread local holder class for Processor class loaders
 *
 * This class is used to access processor class loader in deserialization code, where an object (typically metaSupplier,
 * supplier or a processor instance) needs to be deserialized with processor classloader and there is no access to
 * JobExecutionService, which manages the processor classloaders.
 *
 * The ThreadLocal
 */
public final class ProcessorClassLoaderTLHolder {

    private static final ThreadLocal<Map<String, ClassLoader>> CLASS_LOADERS = ThreadLocal.withInitial(HashMap::new);

    private ProcessorClassLoaderTLHolder() {
    }

    public static ClassLoader get(String key) {
        return CLASS_LOADERS.get().get(key);
    }

    static void putAll(Map<String, ClassLoader> map) {
        CLASS_LOADERS.get().putAll(map);
    }

    static void remove() {
        CLASS_LOADERS.remove();
    }
}
