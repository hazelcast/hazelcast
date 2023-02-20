/*
 * Copyright 2023 Hazelcast Inc.
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

import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class WatermarkedFields implements Serializable {

    private final Set<Integer> fieldIndexes;

    public WatermarkedFields(Set<Integer> fieldIndexes) {
        this.fieldIndexes = Collections.unmodifiableSet(fieldIndexes);
    }

    /**
     * Returns an instance that will have all the fields of {@code this} and
     * {@code other} instance watermarked.
     */
    public WatermarkedFields union(WatermarkedFields other) {
        if (other == null || other.fieldIndexes.isEmpty()) {
            return this;
        }

        Set<Integer> newWatermarkedFields = new HashSet<>(this.fieldIndexes);
        newWatermarkedFields.addAll(other.fieldIndexes);
        assert this.fieldIndexes.size() + other.fieldIndexes.size() == newWatermarkedFields.size();
        return new WatermarkedFields(newWatermarkedFields);
    }

    public int findFirst() {
        return fieldIndexes.iterator().next();
    }

    @Nullable
    public Integer findFirst(ImmutableBitSet indices) {
        for (Integer entry : fieldIndexes) {
            if (indices.get(entry)) {
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
        return Objects.equals(fieldIndexes, that.fieldIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldIndexes);
    }

    public boolean isEmpty() {
        return fieldIndexes.isEmpty();
    }

    public Set<Integer> getFieldIndexes() {
        return fieldIndexes;
    }
}
