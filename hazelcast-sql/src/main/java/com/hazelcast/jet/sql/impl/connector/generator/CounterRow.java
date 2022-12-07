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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.sql.impl.row.Row;

import java.util.Objects;

/**
 * Row with single integer field
 */
class CounterRow implements Row {

    private final int value;

    public CounterRow(int value) {
        this.value = value;
    }

    @Override
    public <T> T get(int index) {
        if (index > 0) {
            throw new IndexOutOfBoundsException("CounterRow has one column");
        }
        return (T) Integer.valueOf(value);
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CounterRow that = (CounterRow) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return getClass().getName() + "{" + value + "}";
    }
}
