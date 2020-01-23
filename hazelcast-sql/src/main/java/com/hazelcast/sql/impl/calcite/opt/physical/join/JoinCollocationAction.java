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
 * Action which should be performed on input to achieve collocation.
 */
public enum JoinCollocationAction {
    /** Input is already positioned correctly. Nothing to do. */
    NONE,

    /** Unicast on join distribution key. */
    UNICAST,

    /** Broadcast on join distribution key. */
    BROADCAST,

    /** Hash replicated input in order to convert it to partitioned form. */
    REPLICATED_HASH;
}
