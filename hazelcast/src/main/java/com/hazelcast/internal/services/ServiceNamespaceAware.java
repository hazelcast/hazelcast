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

package com.hazelcast.internal.services;

import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;

/**
 * An object which is aware of {@link ServiceNamespace} which object itself belongs to.
 * <p>
 * {@link BackupAwareOperation}s and {@link BackupOperation}s created by {@link FragmentedMigrationAwareService}s
 * must implement {@code ServiceNamespaceAware}.
 *
 * @see ServiceNamespace
 * @see FragmentedMigrationAwareService
 * @since 3.9
 */
public interface ServiceNamespaceAware {
    /**
     * Returns the {@link ServiceNamespace} which this object belongs to.
     *
     * @return service namespace
     */
    ServiceNamespace getServiceNamespace();
}
