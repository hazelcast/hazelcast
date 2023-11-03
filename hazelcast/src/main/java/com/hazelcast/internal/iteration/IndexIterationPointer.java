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

package com.hazelcast.internal.iteration;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.query.impl.OrderedIndexStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * Index iteration range or point lookup.
 * <p>
 * The following types of pointers are supported (see also predefined constants in the class):
 * <ul>
 *     <li>unconstrained pointer: (null, null) - scans null and not-null values</li>
 *     <li>single side constrained ranges: (null, X) and (X, null). Note that (null, X)
 *     includes also NULL</li>
 *     <li>constrained range: (X, Y)</li>
 *     <li>point lookup pointer: (X, X)</li>
 *     <li>IS NULL pointer: [NULL, NULL] - scans only NULL values. Special case of point lookup pointer</li>
 *     <li>NOT NULL pointer: (NULL, null)</li>
 * </ul>
 * Important conventions:
 * <ol>
 *     <li>either end can be NULL and NULL end can be inclusive or not.
 *     Inclusive NULL on the left side is equivalent to null (i.e. -Inf).
 *     [null, NULL] is equivalent to  [NULL, NULL]. [null, NULL) should produce empty result.</li>
 *     <li>null can be inclusive or not - both variants are treated in the same way</li>
 *     <li>NULL end does not make sense for composite index because composite index
 *     stores {@link com.hazelcast.query.impl.CompositeValue} which are never null, even if
 *     they consist entirely of {@link com.hazelcast.query.impl.AbstractIndex#NULL} values.</li>
 *     </li>beware of distinction between Java null and NULL - it is not always obvious.</li>
 * </ol>
 * <p>
 * {@link IndexIterationPointer} set operation use the following ordering:
 * {@code -Inf < NULL < non-null value < +Inf}.
 */
public class IndexIterationPointer implements IdentifiedDataSerializable {

    public static final IndexIterationPointer ALL = create(null, false, null, false, false, null);
    public static final IndexIterationPointer ALL_DESC = ALL.asDescending();
    // alternative representation of ALL pointer (valid only for non-composite index)
    public static final IndexIterationPointer ALL_ALT = create(AbstractIndex.NULL, true, null, false, false, null);
    public static final IndexIterationPointer ALL_ALT_DESC = ALL_ALT.asDescending();
    public static final IndexIterationPointer IS_NULL = create(AbstractIndex.NULL, true, AbstractIndex.NULL, true, false, null);
    public static final IndexIterationPointer IS_NULL_DESC = IS_NULL.asDescending();
    public static final IndexIterationPointer IS_NOT_NULL = create(AbstractIndex.NULL, false, null, false, false, null);
    public static final IndexIterationPointer IS_NOT_NULL_DESC = IS_NOT_NULL.asDescending();

    private static final Comparator<IndexIterationPointer> FROM_COMPARATOR =
            Comparator.comparing(IndexIterationPointer::getFrom, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR);
    private static final Comparator<IndexIterationPointer> TO_COMPARATOR =
            Comparator.comparing(IndexIterationPointer::getTo, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR);
    // Note that this ordering is not fully compatible with equals as it ignores inclusiveness.
    // This is sufficient for the purpose of normalization.
    // TODO: revisit in https://hazelcast.atlassian.net/browse/HZ-3093
    //  maybe this is ok because IndexIterationPointer should not (?) contain NativeMemoryData
    private static final Comparator<IndexIterationPointer> POINTER_COMPARATOR = FROM_COMPARATOR.thenComparing(TO_COMPARATOR);
    private static final Comparator<IndexIterationPointer> POINTER_COMPARATOR_REVERSED = POINTER_COMPARATOR.reversed();

    private static final byte FLAG_DESCENDING = 1;
    private static final byte FLAG_FROM_INCLUSIVE = 1 << 1;
    private static final byte FLAG_TO_INCLUSIVE = 1 << 2;
    // Note this flag is misleading, it is also set for unconstrained pointers
    private static final byte FLAG_POINT_LOOKUP = 1 << 3;

    private byte flags;
    private Comparable<?> from;
    private Comparable<?> to;
    private Data lastEntryKeyData;

    public IndexIterationPointer() {
    }

    private IndexIterationPointer(
            byte flags,
            @Nullable Comparable<?> from,
            @Nullable Comparable<?> to,
            @Nullable Data lastEntryKeyData
    ) {
        assert from == null || to == null || ((Comparable) from).compareTo(to) <= 0 : "from must be <= than to";

        this.flags = flags;

        assert from == null || to == null || ((Comparable) from).compareTo(to) != 0
                || (isFromInclusive() && isToInclusive())
                : "Point lookup limits must be all inclusive";

        this.from = from;
        this.to = to;
        this.lastEntryKeyData = lastEntryKeyData;
    }

    public static IndexIterationPointer create(
            @Nullable Comparable<?> from,
            boolean fromInclusive,
            @Nullable Comparable<?> to,
            boolean toInclusive,
            boolean descending,
            @Nullable Data lastEntryKey
    ) {
        return new IndexIterationPointer(
                (byte) ((descending ? FLAG_DESCENDING : 0)
                        | (fromInclusive ? FLAG_FROM_INCLUSIVE : 0)
                        | (toInclusive ? FLAG_TO_INCLUSIVE : 0)
                        | (from == to ? FLAG_POINT_LOOKUP : 0)),
                from,
                to,
                lastEntryKey
        );
    }

    public IndexIterationPointer asDescending() {
        assert !isDescending() : "Pointer is already descending";
        return create(getFrom(), isFromInclusive(), getTo(), isToInclusive(), true, lastEntryKeyData);
    }

    // visible for tests
    boolean isAll() {
        return (from == null || (from == AbstractIndex.NULL && isFromInclusive()))
                &&  (to == null);
    }

    @Nullable
    public Comparable<?> getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return (flags & FLAG_FROM_INCLUSIVE) != 0;
    }

    @Nullable
    public Comparable<?> getTo() {
        return to;
    }

    public boolean isToInclusive() {
        return (flags & FLAG_TO_INCLUSIVE) != 0;
    }

    public boolean isDescending() {
        return (flags & FLAG_DESCENDING) != 0;
    }

    public Data getLastEntryKeyData() {
        return lastEntryKeyData;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(flags);
        out.writeObject(from);
        if ((flags & FLAG_POINT_LOOKUP) == 0) {
            out.writeObject(to);
        }
        IOUtil.writeData(out, lastEntryKeyData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        flags = in.readByte();
        from = in.readObject();
        if ((flags & FLAG_POINT_LOOKUP) == 0) {
            to = in.readObject();
        } else {
            to = from;
        }
        lastEntryKeyData = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.INDEX_ITERATION_POINTER;
    }

    @Override
    public String toString() {
        return "IndexIterationPointer{"
                + (isFromInclusive() ? "[" : "(") + from + ", " + to + (isToInclusive() ? "]" : ")")
                + (isDescending() ? " DESC" : " ASC")
                + ", lastEntryKeyData=" + lastEntryKeyData
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexIterationPointer that = (IndexIterationPointer) o;
        return flags == that.flags
                && Objects.equals(getFrom(), that.getFrom())
                && Objects.equals(getTo(), that.getTo())
                && Objects.equals(getLastEntryKeyData(), that.getLastEntryKeyData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(flags, getFrom(), getTo(), getLastEntryKeyData());
    }

    /**
     * Checks if two pointers overlap or are adjacent which means that they can be combined into a single pointer.
     * Requires that {@code left <= right} regardless of descending flags - leads to simpler checks
     * @param left
     * @param right
     * @param comparator from/to value comparator, must be able to handle special values (NULL, null).
     * @return if the pointers overlap or are adjacent
     */
    public static boolean overlapsOrdered(IndexIterationPointer left, IndexIterationPointer right, Comparator comparator) {
        assert left.isDescending() == right.isDescending() : "Cannot compare pointer with different directions";
        assert left.lastEntryKeyData == null && right.lastEntryKeyData == null : "Can merge only initial pointers";

        // fast path for the same instance
        if (left == right) {
            return true;
        }

        assert comparator.compare(left.from, right.from) <= 0 : "Pointers must be ordered";

        // if one of the ends is +/-inf respectively -> overlap
        if (left.to == null || right.from == null) {
            return true;
        }

        // if given end is equal the ranges overlap (or at least are adjacent)
        // if at least one of the ranges is inclusive
        boolean eqOverlaps = left.isToInclusive() || right.isFromInclusive();

        // Check non-inf values, do not need to check the other way around because pointers are ordered
        // Thanks to order we do not have to check `right.to`, we only need to check
        // if `right.from` belongs to `left` pointer range.
        // we must take into account inclusiveness so we do not merge < X and > X ranges
        int rfCmpLt = comparator.compare(right.from, left.to);
        return eqOverlaps ? rfCmpLt <= 0 : rfCmpLt < 0;
    }

    /**
     * Calculates union of two iteration pointers. If the pointers are not
     * overlapping it will contain also everything between them. Pointers can
     * be passed in any order.
     *
     * @param left
     * @param right
     * @param comparator from/to value comparator, must be able to handle special values (NULL, null).
     * @return
     * @see #overlapsOrdered
     */
    public static IndexIterationPointer union(IndexIterationPointer left, IndexIterationPointer right, Comparator comparator) {
        assert left.isDescending() == right.isDescending();
        assert left.getLastEntryKeyData() == null && right.lastEntryKeyData == null : "Can merge only initial pointers";
        Tuple2<Comparable<?>, Boolean> newFrom = min(left.getFrom(), left.isFromInclusive(),
                right.getFrom(), right.isFromInclusive(),
                comparator);
        Tuple2<Comparable<?>, Boolean> newTo = max(left.getTo(), left.isToInclusive(),
                right.getTo(), right.isToInclusive(),
                comparator);

        return IndexIterationPointer.create(newFrom.f0(), newFrom.f1(), newTo.f0(), newTo.f1(),
                left.isDescending(), null);
    }

    // (value, inclusive); null means -inf
    private static Tuple2<Comparable<?>, Boolean> min(Comparable<?> left, boolean leftInclusive,
                                                      Comparable<?> right, boolean rightInclusive,
                                                      Comparator comparator) {
        // fast path for infinity
        if (left == null || right == null) {
            // -inf is not inclusive
            return tuple2(null, false);
        }
        int result = comparator.compare(left, right);
        if (result == 0) {
            // both ends are equal, but might differ in inclusiveness,
            // use one which covers bigger range
            return tuple2(left, leftInclusive || rightInclusive);
        } else if (result < 0) {
            return tuple2(left, leftInclusive);
        } else {
            return tuple2(right, rightInclusive);
        }
    }

    // (value, inclusive), null means +inf
    private static Tuple2<Comparable<?>, Boolean> max(Comparable<?> left, boolean leftInclusive,
                                                      Comparable<?> right, boolean rightInclusive,
                                                      Comparator comparator) {
        // fast path for infinity
        if (left == null || right == null) {
            // +inf is not inclusive
            return tuple2(null, false);
        }
        int result = comparator.compare(left, right);
        if (result == 0) {
            // both ends are equal, but might differ in inclusiveness,
            // use one which covers bigger range
            return tuple2(left, leftInclusive || rightInclusive);
        } else if (result < 0) {
            return tuple2(right, rightInclusive);
        } else {
            return tuple2(left, leftInclusive);
        }
    }

    /**
     * Converts list of {@link IndexIterationPointer}s to ordered list of non-overlapping pointers.
     *
     * @param result List to be normalized. It may be modified in place
     * @param descending
     * @return Normalized list. It may be the same object as passed as argument or different.
     */
    @Nonnull
    public static List<IndexIterationPointer> normalizePointers(@Nonnull List<IndexIterationPointer> result, boolean descending) {
        if (result.size() <= 1) {
            // single pointer, nothing to do
            return result;
        }

        // without the same ordering of pointers order of results would be unspecified
        assert result.stream().allMatch(r -> r.isDescending() == descending)
                : "All iteration pointers must have the same direction";

        // order of ranges is critical for preserving ordering of the results
        Collections.sort(result, descending ? POINTER_COMPARATOR_REVERSED : POINTER_COMPARATOR);

        // loop until we processed the last remaining pair
        //
        // do the normalization in place without extra shifts in the array
        // we write normalized pointers from the beginning
        int writeIdx = 0;
        IndexIterationPointer currentMerged = result.get(0);
        for (int nextPointerIdx = 1; nextPointerIdx < result.size(); nextPointerIdx++) {
            // compare current pointer with next one and merge if they overlap
            // otherwise go to next pointer
            // pointers might be ordered in descending way but util methods expect ascending order of arguments
            IndexIterationPointer next = result.get(nextPointerIdx);
            if (!descending && overlapsOrdered(currentMerged, next, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)) {
                // merge overlapping ranges
                currentMerged = union(currentMerged, next, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR);
            } else if (descending && overlapsOrdered(next, currentMerged, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)) {
                // merge overlapping ranges
                currentMerged = union(next, currentMerged, OrderedIndexStore.SPECIAL_AWARE_COMPARATOR);
            } else {
                // write current pointer and advance
                result.set(writeIdx++, currentMerged);
                currentMerged = next;
            }
        }
        // write last remaining pointer
        result.set(writeIdx++, currentMerged);

        return result.subList(0, writeIdx);
    }
}
