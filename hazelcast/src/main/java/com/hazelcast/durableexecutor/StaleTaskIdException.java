/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.durableexecutor;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.exception.SilentException;

/**
 * An {@link RuntimeException} that is thrown when retrieving the result of a task via {@link DurableExecutorService} if the
 * result of the task is overwritten. This means the task is executed but the result isn't available anymore
 */
public class StaleTaskIdException extends HazelcastException implements SilentException {

    public StaleTaskIdException(String message) {
        super(message);
    }
}
