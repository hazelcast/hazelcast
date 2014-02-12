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

/**
 * <p>This package contains the MapReduce API definition for Hazelcast.<br/>
 * All map reduce operations running in a distributed manner inside the
 * active Hazelcast cluster. Therefor {@link com.hazelcast.mapreduce.Mapper},
 * {@link com.hazelcast.mapreduce.Combiner} and {@link com.hazelcast.mapreduce.Reducer}
 * implementations need to be fully serializable by Hazelcast. Any of the
 * existing serialization patterns are available for those classes, too.<br/>
 * If custom {@link com.hazelcast.mapreduce.KeyValueSource} is provided above
 * statement also applies to this implementation.</p>
 * <p>For a basic idea how to use this framework see {@link com.hazelcast.mapreduce.Job}
 * or {@link com.hazelcast.mapreduce.Mapper}, {@link com.hazelcast.mapreduce.Combiner} or
 * {@link com.hazelcast.mapreduce.Reducer}.</p>
 *
 * @since 3.2
 */
@Beta package com.hazelcast.mapreduce;

import com.hazelcast.spi.annotation.Beta;
