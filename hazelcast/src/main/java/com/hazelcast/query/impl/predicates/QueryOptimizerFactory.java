/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;

import java.util.Arrays;

/**
 * Creates {@link QueryOptimizer} according to GroupProperties configuration.
 *
 */
public final class QueryOptimizerFactory {
    public enum Type {
        NONE,
        RULES
    }

    private QueryOptimizerFactory() {
    }

    /**
     * Creates new QueryOptimizer. The exact implementation depends on
     * GroupProperties.
     *
     * @param properties
     * @return
     */
    public static QueryOptimizer newOptimizer(GroupProperties properties) {
        GroupProperty property = GroupProperty.QUERY_OPTIMIZER_TYPE;
        String string = properties.getString(property);
        Type type;
        try {
            type = Type.valueOf(string);
        } catch (IllegalArgumentException e) {
            throw onInvalidOptimizerType(string);
        }
        switch (type) {
            case RULES:
                return new RuleBasedQueryOptimizer();
            default:
                return new EmptyOptimizer();
        }
    }

    private static IllegalArgumentException onInvalidOptimizerType(String type) {
        StringBuilder sb = new StringBuilder("Unknown Optimizer Type: ")
                .append(type)
                .append(". Use property '")
                .append(GroupProperty.QUERY_OPTIMIZER_TYPE.getName())
                .append("' to select optimizer. ")
                .append("Available optimizers: ");
        Type[] values = Type.values();
        sb.append(Arrays.toString(values));
        return new IllegalArgumentException(sb.toString());
    }
}
