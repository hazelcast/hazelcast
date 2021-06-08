/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.function.Function;

/**
 * A supplier of {@link EventTimePolicy}.
 */
public interface EventTimePolicySupplier extends IdentifiedDataSerializable {

    EventTimePolicy<Object[]> eventTimePolicy(Function<String, Integer> fieldIndexResolver);

    class LagEventTimePolicySupplier implements EventTimePolicySupplier {

        private String fieldName;
        private long limitingLagMillis;

        public LagEventTimePolicySupplier() {
        }

        public LagEventTimePolicySupplier(String fieldName, long limitingLagMillis) {
            this.fieldName = fieldName;
            this.limitingLagMillis = limitingLagMillis;
        }

        @Override
        public EventTimePolicy<Object[]> eventTimePolicy(Function<String, Integer> fieldIndexResolver) {
            int fieldIndex = fieldIndexResolver.apply(fieldName);
            long limitingLagMillis = this.limitingLagMillis;
            return EventTimePolicy.eventTimePolicy(
                    row -> ((OffsetDateTime) row[fieldIndex]).toInstant().toEpochMilli(),
                    (row, timestamp) -> row,
                    WatermarkPolicy.limitingLag(limitingLagMillis),
                    0,
                    0,
                    0
            );
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.LAG_EVENT_TIME_POLICY_SUPPLIER;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeLong(limitingLagMillis);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            fieldName = in.readString();
            limitingLagMillis = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LagEventTimePolicySupplier that = (LagEventTimePolicySupplier) o;
            return limitingLagMillis == that.limitingLagMillis && Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, limitingLagMillis);
        }
    }
}
