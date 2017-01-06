/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import java.util.concurrent.Future;

/**
 * A Jet computation job created from a {@link DAG},
 * ready to be executed.
 */
public interface Job {
    /**
     * Executes the job.
     *
     * @return a future that can be inspected for job completion status
     * and cancelled to prematurely end the job.
     */
    Future<Void> execute();
}
