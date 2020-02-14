/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.affinity;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ThreadAffinityProperties {

    private static final String PROPERTY_BASE = "hz.%s";
    private static final AtomicBoolean ENABLED = new AtomicBoolean(false);

    private ThreadAffinityProperties() {
    }

    public static boolean isAffinityEnabled() {
        if (ENABLED.get()) {
            return true;
        }

        for (ThreadAffinity.Group group : ThreadAffinity.Group.values()) {
            if (System.getProperty(String.format(PROPERTY_BASE, group.value)) != null) {
                ENABLED.set(true);
                return true;
            }
        }

        return false;
    }

    public static Set<Integer> getCoreIds(ThreadAffinity.Group group) {
        String property = String.format(PROPERTY_BASE, group.value);
        return parseCoreIds(property);
    }

    public static int countCores(ThreadAffinity.Group group) {
        String property = String.format(PROPERTY_BASE, group.value);
        return parseCoreIds(property).size();
    }

    private static Set<Integer> parseCoreIds(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isEmpty() || value.trim().isEmpty()) {
            return Collections.emptySet();
        }

        value = value.trim();

        Set<Integer> cores = new LinkedHashSet<>(0);
        for (String s : value.split(",")) {
            int indexOf = s.indexOf("-");
            if (indexOf >= 0) {
                int from = Integer.parseInt(s.substring(0, indexOf));
                int to = Integer.parseInt(s.substring(indexOf + 1));
                for (int core = from; core <= to; core++) {
                    cores.add(core);
                }
            } else {
                int cpu = Integer.parseInt(s);
                cores.add(cpu);
            }
        }

        return cores;
    }
}
