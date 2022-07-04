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

package com.hazelcast.sql.impl.expression.predicate;

/**
 * Defines comparison modes supported by {@link ComparisonPredicate}.
 */
public enum ComparisonMode {
    EQUALS(0),
    NOT_EQUALS(1),
    GREATER_THAN(2),
    GREATER_THAN_OR_EQUAL(3),
    LESS_THAN(4),
    LESS_THAN_OR_EQUAL(5);

    private static final ComparisonMode[] VALUES = values();

    private final int id;

    ComparisonMode(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static ComparisonMode getById(int id) {
        for (ComparisonMode value : VALUES) {
            if (id == value.id) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown ID: " + id);
    }
}
