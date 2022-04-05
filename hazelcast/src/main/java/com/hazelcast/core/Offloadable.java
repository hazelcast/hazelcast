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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

/**
 * Allows off-loading the processing unit implementing this interface to the specified or default Executor.
 *
 * Currently supported in:
 * <ul>
 *     <li>{@link IMap#executeOnKey(Object, EntryProcessor)}</li>
 *     <li>{@link IMap#submitToKey(Object, EntryProcessor)} </li>
 * </ul>
 */
@FunctionalInterface
public interface Offloadable {

    /**
     * Constant meaning that there will be no off-loading if returned from the {@link #getExecutorName()} method.
     */
    String NO_OFFLOADING = "no-offloading";

    /**
     * Constant meaning that processing will be off-loaded to the default OFFLOADABLE_EXECUTOR executor.
     * if returned from the {@link #getExecutorName()} method.
     *
     * @see ExecutionService#OFFLOADABLE_EXECUTOR
     */
    String OFFLOADABLE_EXECUTOR = ExecutionService.OFFLOADABLE_EXECUTOR;

    /**
     * Returns the name of the executor to which the processing unit will be off-loaded.
     *
     * The return value equal to {@value OFFLOADABLE_EXECUTOR} indicates that the processing should off-loaded to the
     * default {@link ExecutionService#OFFLOADABLE_EXECUTOR}.
     *
     * The return value equal to {@value NO_OFFLOADING} indicates that the processing should not be off-loaded at all.
     * The processing will be executed as if the processing-unit didn't implement the {@link Offloadable} interface.
     *
     * Other return values will lookup the executor with the returned value which can be configured in the Hazelcast
     * configuration.
     *
     * @return the name of the executor to which the processing should be off-loaded.
     * @see ExecutorConfig
     * @see Config#addExecutorConfig(ExecutorConfig)
     */
    String getExecutorName();

}
