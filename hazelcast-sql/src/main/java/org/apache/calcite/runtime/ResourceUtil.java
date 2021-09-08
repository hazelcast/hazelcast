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

package org.apache.calcite.runtime;

/**
 * A class that enables access to protected {@link Resources} members.
 */
public final class ResourceUtil {

    private ResourceUtil() {
    }

    public static String key(Resources.Element element) {
        return element.key;
    }

    public static Object[] args(Resources.Inst instance) {
        return instance.args;
    }
}
