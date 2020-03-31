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
    /** Static mapping: member IDs. */
    private final Collection<UUID> staticMemberIds;

    /** Dynamic mapping: member count. */
    private final int dynamicMemberCount;

    private PlanFragmentMapping(Collection<UUID> staticMemberIds, int dynamicMemberCount) {
        this.staticMemberIds = staticMemberIds;
        this.dynamicMemberCount = dynamicMemberCount;
    }

    public static PlanFragmentMapping staticMapping(Collection<UUID> staticMemberIds) {
        return new PlanFragmentMapping(staticMemberIds, 0);
    }

    // TODO: Make sure to fallback to dynamic approach when there are too many members
    public static PlanFragmentMapping dynamicMapping(int dynamicMemberCount) {
        return new PlanFragmentMapping(null, dynamicMemberCount);
    }

    public boolean isStatic() {
        return staticMemberIds != null;
    }

    public Collection<UUID> getStaticMemberIds() {
        return staticMemberIds;
    }

    public int getDynamicMemberCount() {
        return dynamicMemberCount;
    }

    public int getMemberCount() {
        return isStatic() ? staticMemberIds.size() : dynamicMemberCount;
    }
}
