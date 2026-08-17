/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.MapLoader;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

public class SimpleStringMapLoader implements MapLoader<String, String> {

    final int size;

    public SimpleStringMapLoader(int size) {
        this.size = size;
    }

    @Override
    public String load(String key) {
        return "value" + key;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        return keys.stream().collect(Collectors.toMap(Function.identity(), this::load));
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return range(0, size).mapToObj(i -> "" + i).toList();
    }
}
