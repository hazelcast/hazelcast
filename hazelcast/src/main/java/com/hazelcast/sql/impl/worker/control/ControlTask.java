/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker.control;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.worker.WorkerTask;

/**
 * Control task (start query, stop query, handle cluster events).
 */
public interface ControlTask extends WorkerTask {
    /**
     * @return Query ID or {@code null} in case of event which should be delivered to all stripes.
     */
    QueryId getQueryId();
}
