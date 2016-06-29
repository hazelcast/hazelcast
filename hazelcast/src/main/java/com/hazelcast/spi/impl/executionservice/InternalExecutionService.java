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

package com.hazelcast.spi.impl.executionservice;

import com.hazelcast.spi.ExecutionService;

/**
 * The internal {@link ExecutionService}. Currently this method doesn't contains any additional methods, but
 * in the future they can be added here.
 */
public interface InternalExecutionService extends ExecutionService {


    void executeDurable(String name, Runnable command);

    void shutdownDurableExecutor(String name);
}
