/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.spi.ExecutionService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class MemberNearCacheExecutor implements NearCacheExecutor {

    private ExecutionService executionService;

    MemberNearCacheExecutor(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return executionService.scheduleWithRepetition(command, initialDelay, delay, unit);
    }
}
