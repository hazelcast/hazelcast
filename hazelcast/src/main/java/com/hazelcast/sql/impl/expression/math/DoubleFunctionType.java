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

package com.hazelcast.sql.impl.expression.math;

public enum DoubleFunctionType {
    COS(0),
    SIN(1),
    TAN(2),
    COT(3),
    ACOS(4),
    ASIN(5),
    ATAN(6),
    SQRT(7),
    EXP(8),
    LN(9),
    LOG10(10),
    DEGREES(11),
    RADIANS(12);

    private final int id;

    DoubleFunctionType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static DoubleFunctionType getById(int id) {
        for (DoubleFunctionType value : values()) {
            if (id == value.id) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown ID: " + id);
    }
}
