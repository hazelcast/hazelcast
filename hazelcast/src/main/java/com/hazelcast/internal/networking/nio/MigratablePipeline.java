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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;

/**
 * A NioPipeline that supports migration between {@link NioThread} instances.
 * This API is called by the {@link IOBalancer}.
 */
public interface MigratablePipeline {

    /**
     * Requests the MigratablePipeline to move to the new NioThread. This call will not wait for the
     * migration to complete.
     *
     * This method can be called by any thread, and will probably be called by the
     * {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer}.
     *
     * Call is ignored when pipeline is moving to the same NioThread.
     *
     * @param newOwner the NioThread that is going to own this MigratablePipeline
     */
    void requestMigration(NioThread newOwner);

    /**
     * Get NioThread currently owning this pipeline. Pipeline owner is a thread
     * running this pipeline. {@link IOBalancer} can decide to migrate a pipeline
     * to another owner.
     *
     * @return current owner
     */
    NioThread owner();

    /**
     * Get 'load' recorded by the current pipeline. It can be used to calculate whether
     * this pipeline should be migrated to a different {@link NioThread}
     *
     * @return total load recorded by this pipeline
     */
    long load();
}
