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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.PredicateConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.internal.serialization.SerializationService;

/**
 *
 */
public class PredicateConfigHolder {

    private final String className;
    private final String sql;
    private final Data implementation;

    public PredicateConfigHolder(String className, String sql, Data implementation) {
        this.className = className;
        this.sql = sql;
        this.implementation = implementation;
    }

    public String getClassName() {
        return className;
    }

    public String getSql() {
        return sql;
    }

    public Data getImplementation() {
        return implementation;
    }

    public PredicateConfig asPredicateConfig(SerializationService serializationService) {
        if (className != null) {
            return new PredicateConfig(className);
        } else if (implementation != null) {
            Predicate predicate = serializationService.toObject(implementation);
            return new PredicateConfig(predicate);
        } else {
            return new PredicateConfig();
        }
    }

    public static PredicateConfigHolder of(PredicateConfig config, SerializationService serializationService) {
        return new PredicateConfigHolder(config.getClassName(), config.getSql(),
                serializationService.toData(config.getImplementation()));
    }
}
