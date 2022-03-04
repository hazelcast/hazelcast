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

package org.example.jet.impl.deployment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Used collect results of reading resource from the classpath during tests.
 * It is intentionally put into org.example package to avoid any filtering done by various classloaders.
 */
public class ResourceCollector {

    private static List<String> items = Collections.synchronizedList(new ArrayList<>());

    public static void add(String resource) {
        items.add(resource);
    }

    public static List<String> items() {
        return items;
    }
}
