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

package com.hazelcast.sql.impl.plan.cache;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for plans.
 */
public class PlanCache implements CachedPlanInvalidationCallback {

    private final int maxSize;
    private final ConcurrentHashMap<PlanCacheKey, CachedPlan> plans = new ConcurrentHashMap<>();

    public PlanCache(int maxSize) {
        assert maxSize > 0;

        this.maxSize = maxSize;
    }

    public CachedPlan get(PlanCacheKey key) {
        CachedPlan plan = plans.get(key);

        if (plan != null) {
            plan.onPlanUsed();

            return plan;
        } else {
            return null;
        }
    }

    public void put(PlanCacheKey key, CachedPlan plan) {
        plans.put(key, plan);

        plan.onPlanUsed();

        shrinkIfNeeded();
    }

    public void invalidate(CachedPlan plan) {
        remove(plan);
    }

    public void clear() {
        plans.clear();
    }

    public int size() {
        return plans.size();
    }

    public void check(PlanCheckContext context) {
        plans.values().removeIf(plan -> !plan.isPlanValid(context));
    }

    private void shrinkIfNeeded() {
        int oversize = plans.size() - maxSize;

        if (oversize <= 0) {
            return;
        }

        // Sort plans according to their last used timestamps
        TreeMap<Long, CachedPlan> sorted = new TreeMap<>();

        for (CachedPlan plan : plans.values()) {
            sorted.put(plan.getPlanLastUsed(), plan);
        }

        // Remove oldest plans
        for (CachedPlan plan : sorted.values()) {
            boolean removed = remove(plan);

            if (removed) {
                if (--oversize == 0) {
                    break;
                }
            }
        }
    }

    /**
     * Removes the plan from the cache, decreasing the size.
     *
     * @param plan Plan.
     */
    private boolean remove(CachedPlan plan) {
        return plans.remove(plan.getPlanKey(), plan);
    }
}
