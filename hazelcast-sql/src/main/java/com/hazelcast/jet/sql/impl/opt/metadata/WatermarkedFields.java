/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.metadata;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public final class WatermarkedFields implements Serializable {

    private final Map<Integer, RexNode> propertiesByIndex;

    public WatermarkedFields(Map<Integer, RexNode> propertiesByIndex) {
        this.propertiesByIndex = Collections.unmodifiableMap(propertiesByIndex);
    }

    public WatermarkedFields merge(WatermarkedFields other) {
        if (other == null || other.propertiesByIndex.isEmpty()) {
            return this;
        }

        Map<Integer, RexNode> newPropertiesByIndex = new HashMap<>(this.propertiesByIndex);
        newPropertiesByIndex.putAll(other.propertiesByIndex);
        assert this.propertiesByIndex.size() + other.propertiesByIndex.size() == newPropertiesByIndex.size();
        return new WatermarkedFields(newPropertiesByIndex);
    }

    @Nullable
    public Map.Entry<Integer, RexNode> findFirst() {
        return propertiesByIndex.entrySet().iterator().next();
    }

    @Nullable
    public Map.Entry<Integer, RexNode> findFirst(ImmutableBitSet indices) {
        for (Entry<Integer, RexNode> entry : propertiesByIndex.entrySet()) {
            if (indices.get(entry.getKey())) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkedFields that = (WatermarkedFields) o;
        return Objects.equals(propertiesByIndex, that.propertiesByIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertiesByIndex);
    }

    public boolean isEmpty() {
        return propertiesByIndex.isEmpty();
    }

    public Map<Integer, RexNode> getPropertiesByIndex() {
        return propertiesByIndex;
    }
}
