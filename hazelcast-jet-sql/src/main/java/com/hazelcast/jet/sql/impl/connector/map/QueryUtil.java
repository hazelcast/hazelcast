/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map.Entry;

final class QueryUtil {

    private QueryUtil() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Predicate<Object, Object> toPredicate(
            Object[] left,
            int[] leftEquiJoinIndices,
            int[] rightEquiJoinIndices,
            QueryPath[] rightPaths
    ) {
        PredicateBuilder builder = Predicates.newPredicateBuilder();
        EntryObject entryObject = builder.getEntryObject();
        for (int i = 0; i < leftEquiJoinIndices.length; i++) {
            Comparable leftValue = asComparable(left[leftEquiJoinIndices[i]]);

            // might need a change when/if IS NOT DISTINCT FROM is supported
            if (leftValue == null) {
                return null;
            }

            QueryPath rightPath = rightPaths[rightEquiJoinIndices[i]];

            EntryObject object;
            if (rightPath.isKey()) {
                object = rightPath.isTop()
                        ? entryObject.key()
                        : entryObject.key().get(rightPath.getPath());
            } else {
                object = rightPath.isTop()
                        ? entryObject.get(rightPath.toString())
                        : entryObject.get(QueryPath.VALUE).get(rightPath.getPath());
            }
            if (i == 0) {
                object.equal(leftValue);
            } else {
                builder.and(object.equal(leftValue));
            }
        }
        return builder;
    }

    private static Comparable<?> asComparable(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Comparable) {
            return (Comparable<?>) value;
        } else {
            throw QueryException.error("JOIN not supported for " + value.getClass() + ": not comparable");
        }
    }

    static Projection<Entry<Object, Object>, Object[]> toProjection(
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        return new JoinProjection(rightRowProjectorSupplier);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class JoinProjection
            implements Projection<Entry<Object, Object>, Object[]>, DataSerializable, SerializationServiceAware {

        private KvRowProjector.Supplier rightRowProjectorSupplier;

        private transient InternalSerializationService serializationService;
        private transient Extractors extractors;

        @SuppressWarnings("unused")
        private JoinProjection() {
        }

        private JoinProjection(KvRowProjector.Supplier rightRowProjectorSupplier) {
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
        }

        @Override
        public Object[] transform(Entry<Object, Object> entry) {
            return rightRowProjectorSupplier.get(serializationService, extractors).project(entry);
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = (InternalSerializationService) serializationService;
            this.extractors = Extractors.newBuilder(this.serializationService).build();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rightRowProjectorSupplier);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rightRowProjectorSupplier = in.readObject();
        }
    }
}
