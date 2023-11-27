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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.internal.services.NodeAware;
import com.hazelcast.jet.impl.util.ImdgUtil;
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
import com.hazelcast.security.SecurityContext;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

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
                object = rightPath.isTopLevel()
                        ? entryObject.key()
                        : entryObject.key().get(rightPath.getPath());
            } else {
                object = rightPath.isTopLevel()
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
            boolean compositeIndex,
            boolean descending,
            ExpressionEvalContext evalContext
    ) {
        List<IndexIterationPointer> result = new ArrayList<>();
        createFromIndexFilterInt(indexFilter, compositeIndex, descending, evalContext, result);
        result = IndexIterationPointer.normalizePointers(result, descending);
        return result.toArray(new IndexIterationPointer[0]);
    }

    private static void createFromIndexFilterInt(
            IndexFilter indexFilter,
            boolean compositeIndex,
            boolean descending,
            ExpressionEvalContext evalContext,
            List<IndexIterationPointer> result
    ) {
        if (indexFilter == null) {
            // Full index scan - should include nulls.
            //
            // Note that from=null is treated differently for composite and non-composite index.
            // This is a bit hacky and might be simplified in the future. IndexIterationPointers are updated
            // during scanning and `[null, null] DESC` pointer might become `[null, X] DESC`.
            // However, the latter one is treated as range pointer (<=X) and would not include NULLs!
            // This duality is caused among others by implicit conversion from null to NULL for
            // non-composite indexes but there are also checks which choose getSqlRecordIteratorBatch method variant
            // based on null end (before conversion) meaning different things.
            // The above affects only `from` because NULLs are smaller than any other value and only DESC sort order
            // for which `to` is updated during the scan.
            if (!compositeIndex && descending) {
                result.add(IndexIterationPointer.ALL_ALT_DESC);
            } else {
                result.add(descending ? IndexIterationPointer.ALL_DESC : IndexIterationPointer.ALL);
            }
        }
        if (indexFilter instanceof IndexRangeFilter) {
            IndexRangeFilter rangeFilter = (IndexRangeFilter) indexFilter;

            if (rangeFilter.getFrom() == null && rangeFilter.getTo() == null) {
                // IS NOT NULL range
                assert !compositeIndex : "IS NOT NULL range should not be generated for composite index";
                result.add(descending ? IndexIterationPointer.IS_NOT_NULL_DESC : IndexIterationPointer.IS_NOT_NULL);
                return;
            }

            // Range filter for non-composite index never includes NULLs.
            // Composite index should have both ends specified, and they might cover also NULL values for components
            // but from/to will never be NULL but CompositeValue.
            Comparable<?> from = compositeIndex ? null : NULL;
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

            if (from != null && to != null) {
                int cmp = ((Comparable) from).compareTo(to);
                if (cmp > 0 || (cmp == 0 && (!rangeFilter.isFromInclusive() || !rangeFilter.isToInclusive()))) {
                    // Range scan with from > to would produce empty result.
                    // Range scan which reduces to point lookup (from = to) produces result only
                    // if both ends are inclusive. Otherwise, result would be empty.
                    // Do not create iteration pointer for such scans (as they would be invalid).
                    return;
                }
            }

            result.add(IndexIterationPointer.create(
                    from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), descending, null));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            // Note: this branch is also used for IS NULL, but null value in IndexEqualsFilter
            // is mapped to NULL by getComparable, so we can easily use it in the same way as ordinary values.
            result.add(IndexIterationPointer.create(value, true, value, true, descending, null));
        } else if (indexFilter instanceof IndexCompositeFilter) {
            IndexCompositeFilter inFilter = (IndexCompositeFilter) indexFilter;
            for (IndexFilter filter : inFilter.getFilters()) {
                createFromIndexFilterInt(filter, compositeIndex, descending, evalContext, result);
            }
        }
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class JoinProjection
            implements Projection<Entry<Object, Object>, JetSqlRow>, DataSerializable,
            NodeAware, SerializationServiceAware {

        private KvRowProjector.Supplier rightRowProjectorSupplier;
        private List<Object> arguments;

        private transient Node node;
        private transient ExpressionEvalContext evalContext;
        private transient Extractors extractors;
        private transient SqlSecurityContext ssc;

        private Subject subject;

        @SuppressWarnings("unused")
        private JoinProjection() {
        }

        private JoinProjection(KvRowProjector.Supplier rightRowProjectorSupplier, ExpressionEvalContext evalContext) {
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
            this.evalContext = evalContext;
            this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
            this.arguments = evalContext.getArguments();
            this.subject = evalContext.subject();
        }

        @Override
        public JetSqlRow transform(Entry<Object, Object> entry) {
            return rightRowProjectorSupplier.get(evalContext, extractors).project(entry.getKey(), entry.getValue());
        }

        @Override
        public void setNode(Node node) {
            assert this.node == null || this.node == node : "Unexpected change of Node instance";
            this.node = node;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            assert evalContext == null || evalContext.getSerializationService() == serializationService
                    : "Unexpected change of serialization service";
            assert node != null : "setNode should be called before setSerializationService";
            initContext((InternalSerializationService) serializationService);
        }

        private void initContext(InternalSerializationService iss) {
            if (evalContext != null) {
                // already created. setSerializationService might be invoked multiple times.
                return;
            }

            SecurityContext securityContext = node.securityContext;
            if (securityContext != null) {
                assert subject != null : "Missing subject when security context exists";
                this.ssc = securityContext.createSqlContext(subject);
            } else {
                this.ssc = NoOpSqlSecurityContext.INSTANCE;
            }

            this.evalContext = ExpressionEvalContext.createContext(arguments, node.getNodeEngine(), iss, ssc);
            this.extractors = Extractors.newBuilder(iss).build();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rightRowProjectorSupplier);
            out.writeObject(arguments);
            ImdgUtil.writeSubject(out, subject);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rightRowProjectorSupplier = in.readObject();
            arguments = in.readObject();
            subject = ImdgUtil.readSubject(in);
        }
    }
}
