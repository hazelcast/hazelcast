/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.util.HashUtil;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class CustomObject implements Serializable {

    private final String name;
    private final UUID uuid;
    private final CustomAttribute attribute;

    public CustomObject(String name, UUID uuid, CustomAttribute attribute) {
        this.name = name;
        this.uuid = uuid;
        this.attribute = attribute;
    }

    public String getName() {
        return name;
    }

    public UUID getUuid() {
        return uuid;
    }

    public CustomAttribute getAttribute() {
        return attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CustomObject that = (CustomObject) o;
        if (!Objects.equals(attribute, that.attribute)) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        return Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return HashUtil.hashCode(name, uuid, attribute);
    }
}
