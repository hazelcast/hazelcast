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

package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.services.kinesis.model.HashKeyRange;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

import static java.math.BigInteger.ZERO;
import static java.math.BigInteger.valueOf;

public class HashRange implements Serializable {
    public static final BigInteger MIN_VALUE = ZERO;
    public static final BigInteger MAX_VALUE = valueOf(2).pow(128);
    public static final HashRange DOMAIN = new HashRange(MIN_VALUE, MAX_VALUE);

    private static final long serialVersionUID = 1L;

    private final BigInteger minInclusive;
    private final BigInteger maxExclusive;

    public HashRange(@Nonnull BigInteger minInclusive, @Nonnull BigInteger maxExclusive) {
        if (minInclusive.compareTo(ZERO) < 0) {
            throw new IllegalArgumentException("Partition start can't be negative");
        }
        if (maxExclusive.compareTo(ZERO) <= 0) {
            throw new IllegalArgumentException("Partition end can't be negative or zero");
        }
        if (maxExclusive.compareTo(minInclusive) <= 0) {
            throw new IllegalArgumentException("Partition end can't be smaller or equal to partition start");
        }
        this.minInclusive = Objects.requireNonNull(minInclusive, "minInclusive");
        this.maxExclusive = Objects.requireNonNull(maxExclusive, "maxExclusive");
    }

    public static HashRange range(@Nonnull BigInteger minInclusive, @Nonnull BigInteger maxExclusive) {
        return new HashRange(minInclusive, maxExclusive);
    }

    public static HashRange range(@Nonnull HashKeyRange hashKeyRange) {
        BigInteger startInclusive = new BigInteger(hashKeyRange.getStartingHashKey());
        BigInteger endExclusive = new BigInteger(hashKeyRange.getEndingHashKey()).add(BigInteger.ONE);
        return new HashRange(startInclusive, endExclusive);
    }

    public static HashRange range(long startInclusive, long endExclusive) {
        return new HashRange(BigInteger.valueOf(startInclusive), BigInteger.valueOf(endExclusive));
    }

    public static HashRange range(@Nonnull String startInclusive, @Nonnull String endExclusive) {
        return new HashRange(new BigInteger(startInclusive), new BigInteger(endExclusive));
    }

    /**
     * Return the slice of the hash range for partition {@code index} out of
     * {@code count} partitions.
     */
    public HashRange partition(int index, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be a strictly positive value");
        }
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("Index must be between 0 and " + count);
        }
        BigInteger partitionStart = minInclusive.add(size().multiply(valueOf(index)).divide(valueOf(count)));
        BigInteger partitionEnd = minInclusive.add(size().multiply(valueOf(index + 1)).divide(valueOf(count)));
        return new HashRange(partitionStart, partitionEnd);
    }

    public BigInteger getMinInclusive() {
        return minInclusive;
    }

    public BigInteger getMaxExclusive() {
        return maxExclusive;
    }

    private BigInteger size() {
        return maxExclusive.subtract(minInclusive);
    }

    public boolean contains(BigInteger value) {
        return value.compareTo(minInclusive) >= 0 && value.compareTo(maxExclusive) < 0;
    }

    public boolean isAdjacent(HashRange other) {
        return minInclusive.equals(other.maxExclusive) || maxExclusive.equals(other.minInclusive);
    }

    @Override
    public int hashCode() {
        return minInclusive.hashCode() + 31 * maxExclusive.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HashRange range = (HashRange) obj;
        return minInclusive.equals(range.minInclusive) && maxExclusive.equals(range.maxExclusive);
    }

    @Override
    public String toString() {
        return "HashRange[" + minInclusive + ", " + maxExclusive + ")";
    }

}
