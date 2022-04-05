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

package com.hazelcast.query.impl;

/**
 * Defines comparisons supported by {@link Index#getRecords(Comparison, Comparable)}
 * and {@link IndexStore#getRecords(Comparable)}.
 */
public enum Comparison {

    /**
     * &lt;
     */
    LESS,

    /**
     * &gt;
     */
    GREATER,

    /**
     * &lt;=
     */
    LESS_OR_EQUAL,

    /**
     * &gt;=
     */
    GREATER_OR_EQUAL

}
