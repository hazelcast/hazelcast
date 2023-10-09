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

package com.hazelcast.internal.metrics.impl;

import java.util.Collection;

/**
 * Utility functions for probes.
 */
class ProbeUtils {
    private ProbeUtils() {
    }

    static void flatten(final Class<?> clazz, final Collection<Class<?>> result) {
        result.add(clazz);

        if (clazz.getSuperclass() != null) {
            flatten(clazz.getSuperclass(), result);
        }

        for (final Class<?> interfaze : clazz.getInterfaces()) {
            result.add(interfaze);
            flatten(interfaze, result);
        }
    }
}
