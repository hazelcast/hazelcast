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

package com.hazelcast.sql.impl.calcite.opt.physical.join;

/**
 * Type of join condition.
 */
public enum JoinConditionType {
    /** Equi-join */
    EQUI,

    // TODO: Do we need it at all? Perhaps we need another anum SEMI_ANTI in JoinType?
    /** Anti equi-join */
    ANTI_EQUI,

    /** All other joins, which are neither {@link #EQUI}, nor {@link #ANTI_EQUI} */
    OTHER
}
