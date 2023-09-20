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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.impl.util.Util;
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
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ExpressionEvalContextImpl;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public final class QueryUtil {
    private QueryUtil() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Predicate<Object, Object> toPredicate(
            JetSqlRow left,
            int[] leftEquiJoinIndices,
            int[] rightEquiJoinIndices,
            QueryPath[] rightPaths
    ) {
        PredicateBuilder builder = Predicates.newPredicateBuilder();
        EntryObject entryObject = builder.getEntryObject();
        for (int i = 0; i < leftEquiJoinIndices.length; i++) {
            Comparable leftValue = asComparable(left.get(leftEquiJoinIndices[i]));

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

    static Projection<Entry<Object, Object>, JetSqlRow> toProjection(
            KvRowProjector.Supplier rightRowProjectorSupplier,
            ExpressionEvalContext evalContext
    ) {
        return new JoinProjection(rightRowProjectorSupplier, evalContext);
    }

    static IndexIterationPointer[] indexFilterToPointers(
            IndexFilter indexFilter,
            boolean descending,
            ExpressionEvalContext evalContext
    ) {
        ArrayList<IndexIterationPointer> result = new ArrayList<>();
        createFromIndexFilterInt(indexFilter, descending, evalContext, result);
        return result.toArray(new IndexIterationPointer[0]);
    }

    private static void createFromIndexFilterInt(
            IndexFilter indexFilter,
            boolean descending,
            ExpressionEvalContext evalContext,
            List<IndexIterationPointer> result
    ) {
        if (indexFilter == null) {
            result.add(IndexIterationPointer.create(null, true, null, true, descending, null));
        }
        if (indexFilter instanceof IndexRangeFilter) {
            IndexRangeFilter rangeFilter = (IndexRangeFilter) indexFilter;

            Comparable<?> from = null;
            if (rangeFilter.getFrom() != null) {
                Comparable<?> fromValue = rangeFilter.getFrom().getValue(evalContext);
                // If the index filter has expression like a > NULL, we need to
                // stop creating index iteration pointer because comparison with NULL
                // produces UNKNOWN result.
                if (fromValue == null) {
                    return;
                }
                from = fromValue;
            }

            Comparable<?> to = null;
            if (rangeFilter.getTo() != null) {
                Comparable<?> toValue = rangeFilter.getTo().getValue(evalContext);
                // Same comment above for expressions like a < NULL.
                if (toValue == null) {
                    return;
                }
                to = toValue;
            }

            result.add(IndexIterationPointer.create(
                    from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), descending, null));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            result.add(IndexIterationPointer.create(value, true, value, true, descending, null));
        } else if (indexFilter instanceof IndexCompositeFilter) {
            IndexCompositeFilter inFilter = (IndexCompositeFilter) indexFilter;
            for (IndexFilter filter : inFilter.getFilters()) {
                createFromIndexFilterInt(filter, descending, evalContext, result);
            }
        }
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class JoinProjection
            implements Projection<Entry<Object, Object>, JetSqlRow>, DataSerializable,
            HazelcastInstanceAware, SerializationServiceAware {

        private KvRowProjector.Supplier rightRowProjectorSupplier;
        private List<Object> arguments;

        private transient HazelcastInstance hzInstance;
        private transient ExpressionEvalContext evalContext;
        private transient Extractors extractors;

        @SuppressWarnings("unused")
        private JoinProjection() {
        }

        private JoinProjection(KvRowProjector.Supplier rightRowProjectorSupplier, ExpressionEvalContext evalContext) {
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
            this.evalContext = evalContext;
            this.arguments = evalContext.getArguments();
        }

        @Override
        public JetSqlRow transform(Entry<Object, Object> entry) {
            return rightRowProjectorSupplier.get(evalContext, extractors).project(entry.getKey(), entry.getValue());
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hzInstance = hazelcastInstance;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.evalContext = new ExpressionEvalContextImpl(
                    arguments,
                    (InternalSerializationService) serializationService,
                    Util.getNodeEngine(hzInstance));
            this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rightRowProjectorSupplier);
            out.writeObject(arguments);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rightRowProjectorSupplier = in.readObject();
            arguments = in.readObject();
        }
    }
}
