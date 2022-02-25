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

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * {@code ServiceNamespace} is a namespace to group objects, structures, fragments within a service.
 *
 * @since 3.9
 */
public interface ServiceNamespace extends DataSerializable {
    /**
     * Name of the service
     *
     * @return name of the service
     */
    String getServiceName();
}
