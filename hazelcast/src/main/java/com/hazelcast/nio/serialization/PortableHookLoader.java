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

package com.hazelcast.nio.serialization;

import com.hazelcast.logging.Logger;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collection;
import java.util.HashSet;


/**
 * @author mdogan 5/8/13
 */
final class PortableHookLoader {

    private static final String FACTORY_ID = "com.hazelcast.PortableHook";

    private final Map<Integer, ? extends PortableFactory> configuredFactories;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();
    private final Collection<ClassDefinition> definitions = new HashSet<ClassDefinition>();
    private final ClassLoader classLoader;

    PortableHookLoader(Map<Integer, ? extends PortableFactory> configuredFactories, ClassLoader classLoader) {
        this.configuredFactories = configuredFactories;
        this.classLoader = classLoader;
        load();
    }

    private void load() {
        try {
            final Iterator<PortableHook> hooks = ServiceLoader.iterator(PortableHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                PortableHook hook = hooks.next();
                final PortableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
                final Collection<ClassDefinition> defs = hook.getBuiltinDefinitions();
                if (defs != null && !defs.isEmpty()) {
                    definitions.addAll(defs);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (configuredFactories != null) {
            for (Map.Entry<Integer, ? extends PortableFactory> entry : configuredFactories.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    Map<Integer, PortableFactory> getFactories() {
        return factories;
    }

    Collection<ClassDefinition> getDefinitions() {
        return definitions;
    }

    private void register(int factoryId, PortableFactory factory) {
        final PortableFactory current = factories.get(factoryId);
        if (current != null) {
            if (current.equals(factory)) {
                Logger.getLogger(getClass()).warning("PortableFactory[" + factoryId + "] is already registered! Skipping "
                        + factory);
            } else {
                throw new IllegalArgumentException("PortableFactory[" + factoryId
                        + "] is already registered! " + current + " -> " + factory);
            }
        } else {
            factories.put(factoryId, factory);
        }
    }

}
