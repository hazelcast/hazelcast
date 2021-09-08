/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.sql.impl.schema.MappingField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.concat;

final class Metadata {

    private final List<MappingField> fields;
    private final Map<String, String> options;

    Metadata(Map<String, String> options) {
        this(emptyList(), options);
    }

    Metadata(List<MappingField> fields, Map<String, String> options) {
        this.fields = fields;
        this.options = options;
    }

    List<MappingField> fields() {
        return fields;
    }

    Map<String, String> options() {
        return options;
    }

    Metadata merge(Metadata other) {
        List<MappingField> fields = new ArrayList<>(concat(this.fields.stream(), other.fields.stream())
                .collect(toCollection(() -> new TreeSet<>(Comparator.comparing(MappingField::name)))));
        Map<String, String> options = concat(this.options.entrySet().stream(), other.options.entrySet().stream())
                .collect(LinkedHashMap::new, (map, entry) -> map.putIfAbsent(entry.getKey(), entry.getValue()), Map::putAll);
        return new Metadata(fields, options);
    }
}
