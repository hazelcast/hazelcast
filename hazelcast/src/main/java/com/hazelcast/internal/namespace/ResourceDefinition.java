/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace;

import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public interface ResourceDefinition extends IdentifiedDataSerializable {

    /**
     * Returns the identifier of this resource. For example, for a {@code CLASS} type resource it can be the fully-qualified
     * name of the class.
     * @return the identifier of this resource.
     */
    String id();

    /**
     * @return the type of the resource.
     */
    ResourceType type();

    /**
     * @return the contents of the resource.
     */
    byte[] payload();

    /**
     *
     * @return the path of the resource
     */
    String url();
}
