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

package com.hazelcast.config;

// TODO: From Matko:
// TODO: https://github.com/hazelcast/hazelcast/blob/108939ae3c5077d91adc134d87620dde5990cfeb/hazelcast/src/main/resources/hazelcast-config-4.0.xsd#L1743-L1747
// TODO: https://github.com/hazelcast/hazelcast/blob/108939ae3c5077d91adc134d87620dde5990cfeb/hazelcast/src/main/resources/hazelcast-config-4.0.xsd#L3590-L3650

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

// TODO: Description.
public abstract class IndexConfig implements IdentifiedDataSerializable {
    protected String name;

    protected IndexConfig() {
        // No-op.
    }

    public String getName() {
        return name;
    }

    public IndexConfig setName(String name) {
        this.name = name;

        return this;
    }
}
