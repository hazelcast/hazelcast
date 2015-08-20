/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import java.util.concurrent.ScheduledFuture;

/**
 * This interface is used to give {@link com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore} implementations
 * a chance to register themself to being cleaned up from expired entries
 */
public interface CleanerRegistrator {

    <V> ScheduledFuture<V> registerCleaner(ReplicatedRecordStore replicatedRecordStorage);

}
