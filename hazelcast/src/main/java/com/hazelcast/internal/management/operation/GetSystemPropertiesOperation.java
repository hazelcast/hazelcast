/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

public class GetSystemPropertiesOperation extends AbstractLocalOperation {
    private List<Entry<String, String>> properties = new ArrayList<>();

    @Override
    public void run() throws Exception {
        for (Entry<Object, Object> entry : System.getProperties().entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            properties.add(new SimpleEntry<>(Objects.toString(key), Objects.toString(value)));
        }
    }

    @Override
    public Object getResponse() {
        return properties;
    }
}
