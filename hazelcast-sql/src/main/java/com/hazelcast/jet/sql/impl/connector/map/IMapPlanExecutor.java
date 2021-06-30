/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.map.IMapPlan.IMapDeletePlan;
import com.hazelcast.map.impl.EntryRemovingProcessor;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRow;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

class IMapPlanExecutor {

    private final HazelcastInstance hazelcastInstance;

    IMapPlanExecutor(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    SqlResult execute(IMapDeletePlan plan, List<Object> arguments, long timeout) {
        assert arguments.isEmpty();

        String mapName = plan.mapName();
        Expression<?> condition = plan.condition();

        Object key = condition.eval(EmptyRow.INSTANCE, SimpleExpressionEvalContext.from(hazelcastInstance));
        CompletableFuture<Void> future = hazelcastInstance.getMap(mapName)
                .submitToKey(key, EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR)
                .toCompletableFuture();
        try {
            if (timeout > 0) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while deleting an entry");
        } catch (InterruptedException | ExecutionException e) {
            throw sneakyThrow(e);
        }
        return SqlResultImpl.createUpdateCountResult(0);
    }
}
