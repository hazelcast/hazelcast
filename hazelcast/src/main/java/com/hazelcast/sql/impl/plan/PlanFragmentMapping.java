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

package com.hazelcast.sql.impl.plan;

import java.util.Collection;
import java.util.UUID;

/**
 * Fragment mapping: static or dynamic.
 */
public final class PlanFragmentMapping {
    /** Explicit member IDs. */
    private final Collection<UUID> memberIds;

    /** Flag indicating that the fragment should be deployed to data members. */
    private final boolean dataMembers;

    public PlanFragmentMapping(Collection<UUID> memberIds, boolean dataMembers) {
        this.memberIds = memberIds;
        this.dataMembers = dataMembers;
    }

    public Collection<UUID> getMemberIds() {
        return memberIds;
    }

    public boolean isDataMembers() {
        return dataMembers;
    }
}
