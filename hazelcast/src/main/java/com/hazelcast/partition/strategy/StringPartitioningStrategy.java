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

package com.hazelcast.partition.strategy;

import com.hazelcast.core.PartitioningStrategy;

public class StringPartitioningStrategy implements PartitioningStrategy {

    public static final StringPartitioningStrategy INSTANCE = new StringPartitioningStrategy();

    @Override
    public Object getPartitionKey(Object key) {
        if (key instanceof String) {
            return getPartitionKey((String) key);
        }
        return null;
    }

    public static String getBaseName(String name) {
        if (name == null) {
            return null;
        }
        int indexOf = name.indexOf('@');
        if (indexOf == -1) {
            return name;
        }
        return name.substring(0, indexOf);
    }

    public static String getPartitionKey(String key) {
        if (key == null) {
            return null;
        }

        int firstIndexOf = key.indexOf('@');
        if (firstIndexOf == -1) {
            return key;
        } else {
            return key.substring(firstIndexOf + 1);
        }
    }
}
