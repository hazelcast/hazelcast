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

package com.hazelcast.sql;

/**
 * SQL row.
 */
public interface SqlRow {
    /**
     * Gets the value of the column by index.
     * <p>
     * The class of the returned value depends on the SQL type of the column.
     *
     * @see #getRowMetadata()
     * @see SqlColumnMetadata#getType()
     * @param columnIndex Column index, 0-based.
     * @return Value of the column.
     * @throws IndexOutOfBoundsException If column index is out of bounds.
     */
    Object getObject(int columnIndex);

    /**
     * @return Row metadata.
     */
    SqlRowMetadata getRowMetadata();
}
