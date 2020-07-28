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
 * Possible types of result from a query execution.
 */
public enum SqlResultType {
    /**
     * The result is a list of rows. Use {@link SqlResult#iterator()} to
     * iterate them. Typically returned for {@code SELECT} queries.
     */
    ROWS,

    /**
     * There SQL statement produced no result, for example a DDL statement
     * ({@code CREATE}, {@code DROP} etc. ...}).
     */
    VOID
}
