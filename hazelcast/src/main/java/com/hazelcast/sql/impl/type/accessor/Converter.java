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

package com.hazelcast.sql.impl.type.accessor;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.GenericType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Interface to convert an item from one type to another.
 */
public abstract class Converter {
    /**
     * @return Class of the input.
     */
    public abstract Class getClazz();

    /**
     * @return Matching generic type.
     */
    public abstract GenericType getGenericType();

    public boolean asBit(Object val) {
        throw cannotConvertImplicit(val);
    }

    public byte asTinyInt(Object val) {
        throw cannotConvertImplicit(val);
    }

    public short asSmallInt(Object val) {
        throw cannotConvertImplicit(val);
    }

    public int asInt(Object val) {
        throw cannotConvertImplicit(val);
    }

    public long asBigInt(Object val) {
        throw cannotConvertImplicit(val);
    }

    public BigDecimal asDecimal(Object val) {
        throw cannotConvertImplicit(val);
    }

    public float asReal(Object val) {
        throw cannotConvertImplicit(val);
    }

    public double asDouble(Object val) {
        throw cannotConvertImplicit(val);
    }

    public String asVarchar(Object val) {
        throw cannotConvertImplicit(val);
    }

    public LocalDate asDate(Object val) {
        throw cannotConvertImplicit(val);
    }

    public LocalTime asTime(Object val) {
        throw cannotConvertImplicit(val);
    }

    public LocalDateTime asTimestamp(Object val) {
        throw cannotConvertImplicit(val);
    }

    public OffsetDateTime asTimestampWithTimezone(Object val) {
        throw cannotConvertImplicit(val);
    }

    protected HazelcastSqlException cannotConvertImplicit(Object val) {
        throw new HazelcastSqlException(-1, "Cannot implicitly convert a value to " + getGenericType() + ": " + val);
    }
}
