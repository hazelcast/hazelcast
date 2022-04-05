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

package com.hazelcast.sql;

import javax.annotation.Nonnull;

/**
 * SQL row.
 */
public interface SqlRow {
    /**
     * Gets the value of the column by index.
     * <p>
     * The class of the returned value depends on the SQL type of the column. No implicit conversions are performed on the value.
     *
     * @param columnIndex column index, zero-based.
     * @return value of the column
     *
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     * @throws ClassCastException if the type of the column type isn't assignable to the type {@code T}
     *
     * @see #getMetadata()
     * @see SqlColumnMetadata#getType()
     */
    <T> T getObject(int columnIndex);

    /**
     * Gets the value of the column by column name.
     * <p>
     * Column name should be one of those defined in {@link SqlRowMetadata}, case-sensitive. You may also use
     * {@link SqlRowMetadata#findColumn(String)} to test for column existence.
     * <p>
     * The class of the returned value depends on the SQL type of the column. No implicit conversions are performed on the value.
     *
     * @param columnName column name
     * @return value of the column
     *
     * @throws NullPointerException if column name is null
     * @throws IllegalArgumentException if a column with the given name is not found
     * @throws ClassCastException if the type of the column type isn't assignable to the type {@code T}
     *
     * @see #getMetadata()
     * @see SqlRowMetadata#findColumn(String)
     * @see SqlColumnMetadata#getName()
     * @see SqlColumnMetadata#getType()
     */
    <T> T getObject(@Nonnull String columnName);

    /**
     * Gets row metadata.
     *
     * @return row metadata
     *
     * @see SqlRowMetadata
     */
    @Nonnull
    SqlRowMetadata getMetadata();
}
