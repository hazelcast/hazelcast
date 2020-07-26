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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache for plans.
 */
public class SqlPlanCache implements SqlCacheablePlanInvalidationCallback {

    private final int maxSize;

    private final ConcurrentHashMap<String, SqlCacheablePlan> plans = new ConcurrentHashMap<>();
    private final AtomicInteger size = new AtomicInteger();

    public SqlPlanCache(int maxSize) {
        assert maxSize > 0;

        this.maxSize = maxSize;
    }

    public SqlCacheablePlan get(String sql) {
        SqlCacheablePlan plan = plans.get(sql);

        if (plan != null) {
            plan.onPlanUsed();

            return plan;
        } else {
            return null;
        }
    }

    public void set(String sql, SqlCacheablePlan plan) {
        while (true) {
             SqlCacheablePlan existingPlan = plans.get(sql);

             if (existingPlan != null && existingPlan.getPlanCreationTimestamp() > plan.getPlanCreationTimestamp()) {
                 // Do not overwrite the newer plan with the older one.
                 return;
             }

             plan.onPlanUsed();

             if (existingPlan == null) {
                 if (plans.putIfAbsent(sql, plan) == null) {
                     int size0 = size.incrementAndGet();

                     if (size0 > maxSize) {
                         shrink();
                     }

                     return;
                 }
             } else if (plans.replace(sql, existingPlan, plan)) {
                 return;
             }
        }
    }

    public void invalidate(SqlCacheablePlan plan) {
        boolean removed = plans.remove(plan.getPlanSql(), plan);

        if (removed) {
            size.decrementAndGet();
        }
    }

    public void clear() {
        plans.clear();
    }

    private void shrink() {
        int oversize = plans.size() - maxSize;

        if (oversize <= 0) {
            return;
        }

        TreeMap<Long, String> sorted = new TreeMap<>();

        for (Map.Entry<String, SqlCacheablePlan> planEntry : plans.entrySet()) {
            String sql = planEntry.getKey();
            SqlCacheablePlan plan = planEntry.getValue();
            long lastUsed = plan.getPlanLastUsed();

            sorted.put(lastUsed, sql);
        }

        for (String sql : sorted.values()) {
            SqlCacheablePlan removedPlan = plans.remove(sql);

            if (removedPlan != null) {
                size.decrementAndGet();
            }

            oversize--;

            if (oversize == 0) {
                break;
            }
        }
    }
}
