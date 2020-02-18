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

package com.hazelcast.sql.impl;

/**
 * Enumeration which defines query mapping.
 */
public enum QueryFragmentMapping {
    // TODO: We must add another mode when certain fragment is executed only on the given subset of nodes. This is important
    //  for large topologies, where otherwise a distirbuted exchange will trigger N*N streams, where N is number of participants.

    /** Fragment should be executed on all data members. */
    DATA_MEMBERS(0),

    /** Fragment should be executed only on a root member. */
    ROOT(1);

    private final int id;

    QueryFragmentMapping(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static QueryFragmentMapping getById(final int id) {
        for (QueryFragmentMapping type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
