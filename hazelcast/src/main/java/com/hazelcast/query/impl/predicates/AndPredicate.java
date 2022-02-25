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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;
import static com.hazelcast.query.impl.predicates.PredicateUtils.estimatedSizeOf;

/**
 * And Predicate
 */
@BinaryInterface
public final class AndPredicate
        implements IndexAwarePredicate, IdentifiedDataSerializable, VisitablePredicate, NegatablePredicate, CompoundPredicate {

    private static final long serialVersionUID = 1L;

    protected Predicate[] predicates;

    public AndPredicate() {
    }

    public AndPredicate(Predicate... predicates) {
        this.predicates = predicates;
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        Predicate[] result = VisitorUtils.acceptVisitor(predicates, visitor, indexes);
        if (result != predicates) {
            //inner predicates were modified by a visitor
            AndPredicate newPredicate = new AndPredicate(result);
            return visitor.visit(newPredicate, indexes);
        }
        return visitor.visit(this, indexes);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Set<QueryableEntry> smallestResultSet = null;
        List<Set<QueryableEntry>> otherResultSets = null;
        List<Predicate> unindexedPredicates = null;

        for (Predicate predicate : predicates) {
            if (isIndexedPredicate(predicate, queryContext)) {
                // Avoid checking indexed partitions count twice to avoid
                // scenario when the owner partitions count changes concurrently and null
                // value from the filter method may indicate that the index is under
                // construction.
                int ownedPartitionsCount = queryContext.getOwnedPartitionCount();
                queryContext.setOwnedPartitionCount(SKIP_PARTITIONS_COUNT_CHECK);
                Set<QueryableEntry> currentResultSet = ((IndexAwarePredicate) predicate).filter(queryContext);
                queryContext.setOwnedPartitionCount(ownedPartitionsCount);
                if (smallestResultSet == null) {
                    smallestResultSet = currentResultSet;
                } else if (estimatedSizeOf(currentResultSet) < estimatedSizeOf(smallestResultSet)) {
                    otherResultSets = initOrGetListOf(otherResultSets);
                    otherResultSets.add(smallestResultSet);
                    smallestResultSet = currentResultSet;
                } else {
                    otherResultSets = initOrGetListOf(otherResultSets);
                    otherResultSets.add(currentResultSet);
                }
            } else {
                unindexedPredicates = initOrGetListOf(unindexedPredicates);
                unindexedPredicates.add(predicate);
            }
        }

        if (smallestResultSet == null) {
            return null;
        }
        return new AndResultSet(smallestResultSet, otherResultSets, unindexedPredicates);
    }

    private static boolean isIndexedPredicate(Predicate predicate, QueryContext queryContext) {
        return predicate instanceof IndexAwarePredicate
                && ((IndexAwarePredicate) predicate).isIndexed(queryContext);
    }

    private static <T> List<T> initOrGetListOf(List<T> list) {
        if (list == null) {
            list = new LinkedList<T>();
        }
        return list;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        for (Predicate predicate : predicates) {
            if (predicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                if (iap.isIndexed(queryContext)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        for (Predicate predicate : predicates) {
            if (!predicate.apply(mapEntry)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append('(');
        int size = predicates.length;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            sb.append(predicates[i]);
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(predicates.length);
        for (Predicate predicate : predicates) {
            out.writeObject(predicate);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        predicates = new Predicate[size];
        for (int i = 0; i < size; i++) {
            predicates[i] = in.readObject();
        }
    }

    @Override
    public Predicate negate() {
        int size = predicates.length;
        Predicate[] inners = new Predicate[size];
        for (int i = 0; i < size; i++) {
            Predicate original = predicates[i];
            Predicate negated;
            if (original instanceof NegatablePredicate) {
                negated = ((NegatablePredicate) original).negate();
            } else {
                negated = new NotPredicate(original);
            }
            inners[i] = negated;
        }
        OrPredicate orPredicate = new OrPredicate(inners);
        return orPredicate;
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.AND_PREDICATE;
    }

    /**
     * Visitable predicates are treated as effectively immutable, therefore callers should not make any changes to
     * the returned array.
     */
    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public <K, V> Predicate<K, V>[] getPredicates() {
        return predicates;
    }

    /**
     * Visitable predicates are treated as effectively immutable, therefore callers should not make any changes to
     * the array passed as argument after is has been set.
     *
     * @param predicates the array of sub-predicates for this {@code And} operator. It is not safe to make any changes to
     *                   this array after it has been set.
     */
    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public <K, V> void setPredicates(Predicate<K, V>[] predicates) {
        if (this.predicates == null) {
            this.predicates = predicates;
        } else {
            throw new IllegalStateException("Cannot reset predicates in an AndPredicate after they have been already set.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AndPredicate that = (AndPredicate) o;

        return Arrays.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }
}
