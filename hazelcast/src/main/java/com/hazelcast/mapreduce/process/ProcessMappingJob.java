/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.process;

import com.hazelcast.core.CompletableFuture;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.Reducer;

import java.util.Map;

public interface ProcessMappingJob<KeyIn, ValueIn> extends ProcessJob<KeyIn, ValueIn> {

    <ValueOut> ProcessMappingJob<KeyIn, ValueOut> combiner(Combiner<KeyIn, ValueIn, ValueOut> combiner);

    <ValueOut> ProcessReducingJob<KeyIn, ValueOut> reducer(Reducer<KeyIn, ValueIn, ValueOut> reducer);

    CompletableFuture<Map<KeyIn, ValueIn>> submit();

    <ValueOut> CompletableFuture<ValueOut> submit(Collator<Map<KeyIn, ValueIn>, ValueOut> collator);

}
