/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 * Portions Copyright 2014 the original author or authors.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.memory.ByteAccessStrategy;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.Math.abs;

/**
 * Utility methods related to hash tables.
 */
@SuppressFBWarnings({"SF_SWITCH_FALLTHROUGH", "SF_SWITCH_NO_DEFAULT"})
@SuppressWarnings({
        "checkstyle:magicnumber",
        "checkstyle:methodname",
        "checkstyle:fallthrough",
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:booleanexpressioncomplexity",
        "checkstyle:methodlength"})
public final class HashUtil {

    private static final int MURMUR32_BLOCK_SIZE = 4;
    private static final int MURMUR64_BLOCK_SIZE = 16;
    private static final int DEFAULT_MURMUR_SEED = 0x01000193;
    private static final int[] PERTURBATIONS = new int[Integer.SIZE];

    private static final LoadStrategy<byte[]> BYTE_ARRAY_LOADER = new ByteArrayLoadStrategy();
    private static final LoadStrategy<MemoryAccessor> WIDE_DIRECT_LOADER = new WideDirectLoadStrategy();
    private static final LoadStrategy<MemoryAccessor> NARROW_DIRECT_LOADER = new NarrowDirectLoadStrategy();

    static {
        final int primeDisplacement = 17;
        for (int i = 0; i < PERTURBATIONS.length; i++) {
            PERTURBATIONS[i] = MurmurHash3_fmix(primeDisplacement + i);
        }
    }

    private HashUtil() {
    }

    /**
     * Returns the MurmurHash3_x86_32 hash of a block inside a byte array.
     */
    public static int MurmurHash3_x86_32(byte[] data, int offset, int len) {
        final long endIndex = (long) offset + len - 1;
        assert endIndex >= Integer.MIN_VALUE && endIndex <= Integer.MAX_VALUE
                : String.format("offset %,d len %,d would cause int overflow", offset, len);
        return MurmurHash3_x86_32(BYTE_ARRAY_LOADER, data, offset, len, DEFAULT_MURMUR_SEED);
    }

    public static int MurmurHash3_x86_32_direct(long base, int offset, int len) {
        return MurmurHash3_x86_32_direct(MEM, base, offset, len);
    }

    /**
     * Returns the {@code MurmurHash3_x86_32} hash of a memory block accessed by the provided {@link MemoryAccessor}.
     * The {@code MemoryAccessor} will be used to access {@code int}-sized data at addresses {@code (base + offset)},
     * {@code (base + offset + 4)}, etc. The caller must ensure that the {@code MemoryAccessor} supports it, especially
     * when {@code (base + offset)} is not guaranteed to be 4 byte-aligned.
     */
    public static int MurmurHash3_x86_32_direct(MemoryAccessor mem, long base, int offset, int len) {
        return MurmurHash3_x86_32(mem.isBigEndian() ? NARROW_DIRECT_LOADER : WIDE_DIRECT_LOADER,
                mem, base + offset, len, DEFAULT_MURMUR_SEED);
    }

    private static <R> int MurmurHash3_x86_32(LoadStrategy<R> loader, R resource, long offset, int len, int seed) {
        // (len & ~(MURMUR32_BLOCK_SIZE - 1)) is the length rounded down to the Murmur32 block size boundary
        final long tailStart = offset + (len & ~(MURMUR32_BLOCK_SIZE - 1));

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;

        for (long blockAddr = offset; blockAddr < tailStart; blockAddr += MURMUR32_BLOCK_SIZE) {
            // little-endian load order
            int k1 = loader.getInt(resource, blockAddr);
            k1 *= c1;
            // ROTL32(k1,15);
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 *= c2;

            h1 ^= k1;
            // ROTL32(h1,13);
            h1 = (h1 << 13) | (h1 >>> 19);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (loader.getByte(resource, tailStart + 2) & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (loader.getByte(resource, tailStart + 1) & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= loader.getByte(resource, tailStart) & 0xff;
                k1 *= c1;
                // ROTL32(k1,15);
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        // finalization
        h1 ^= len;
        h1 = MurmurHash3_fmix(h1);
        return h1;
    }

    /**
     * Returns the MurmurHash3_x86_32 hash of a block inside a byte array.
     */
    public static long MurmurHash3_x64_64(byte[] data, int offset, int len) {
        return MurmurHash3_x64_64(BYTE_ARRAY_LOADER, data, offset, len, DEFAULT_MURMUR_SEED);
    }

    public static long MurmurHash3_x64_64_direct(long base, int offset, int len) {
        return MurmurHash3_x64_64_direct(MEM, base, offset, len);
    }

    /**
     * Returns the {@code MurmurHash3_x64_64} hash of a memory block accessed by the provided {@link MemoryAccessor}.
     * The {@code MemoryAccessor} will be used to access {@code long}-sized data at addresses {@code (base + offset)},
     * {@code (base + offset + 8)}, etc. The caller must ensure that the {@code MemoryAccessor} supports it, especially
     * when {@code (base + offset)} is not guaranteed to be 8 byte-aligned.
     */
    public static long MurmurHash3_x64_64_direct(MemoryAccessor mem, long base, int offset, int len) {
        return MurmurHash3_x64_64(mem.isBigEndian() ? NARROW_DIRECT_LOADER : WIDE_DIRECT_LOADER,
                mem, base + offset, len, DEFAULT_MURMUR_SEED);
    }

    static <R> long MurmurHash3_x64_64(LoadStrategy<R> loader, R resource, long offset, int len) {
        return MurmurHash3_x64_64(loader, resource, offset, len, DEFAULT_MURMUR_SEED);
    }

    static <R> long MurmurHash3_x64_64(LoadStrategy<R> loader, R resource, long offset, int len, final int seed) {

        // (len & ~(MURMUR64_BLOCK_SIZE - 1)) is the length rounded down to the Murmur64 block boundary
        final long tailStart = offset + (len & ~(MURMUR64_BLOCK_SIZE - 1));

        long h1 = 0x9368e53c2f6af274L ^ seed;
        long h2 = 0x586dcd208f7cd3fdL ^ seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        long k1;
        long k2;

        for (long blockAddr = offset; blockAddr < tailStart; blockAddr += MURMUR64_BLOCK_SIZE) {
            k1 = loader.getLong(resource, blockAddr);
            k2 = loader.getLong(resource, blockAddr + 8);
            // bmix(state);
            k1 *= c1;
            k1 = (k1 << 23) | (k1 >>> 64 - 23);
            k1 *= c2;
            h1 ^= k1;
            h1 += h2;

            h2 = (h2 << 41) | (h2 >>> 64 - 41);

            k2 *= c2;
            k2 = (k2 << 23) | (k2 >>> 64 - 23);
            k2 *= c1;
            h2 ^= k2;
            h2 += h1;

            h1 = h1 * 3 + 0x52dce729;
            h2 = h2 * 3 + 0x38495ab5;

            c1 = c1 * 5 + 0x7b7d159c;
            c2 = c2 * 5 + 0x6bce6396;
        }

        k1 = 0;
        k2 = 0;

        switch (len & 15) {
            case 15:
                k2 ^= (long) loader.getByte(resource, tailStart + 14) << 48;
            case 14:
                k2 ^= (long) loader.getByte(resource, tailStart + 13) << 40;
            case 13:
                k2 ^= (long) loader.getByte(resource, tailStart + 12) << 32;
            case 12:
                k2 ^= (long) loader.getByte(resource, tailStart + 11) << 24;
            case 11:
                k2 ^= (long) loader.getByte(resource, tailStart + 10) << 16;
            case 10:
                k2 ^= (long) loader.getByte(resource, tailStart + 9) << 8;
            case 9:
                k2 ^= loader.getByte(resource, tailStart + 8);

            case 8:
                k1 ^= (long) loader.getByte(resource, tailStart + 7) << 56;
            case 7:
                k1 ^= (long) loader.getByte(resource, tailStart + 6) << 48;
            case 6:
                k1 ^= (long) loader.getByte(resource, tailStart + 5) << 40;
            case 5:
                k1 ^= (long) loader.getByte(resource, tailStart + 4) << 32;
            case 4:
                k1 ^= (long) loader.getByte(resource, tailStart + 3) << 24;
            case 3:
                k1 ^= (long) loader.getByte(resource, tailStart + 2) << 16;
            case 2:
                k1 ^= (long) loader.getByte(resource, tailStart + 1) << 8;
            case 1:
                k1 ^= loader.getByte(resource, tailStart);

                // bmix();
                k1 *= c1;
                k1 = (k1 << 23) | (k1 >>> 64 - 23);
                k1 *= c2;
                h1 ^= k1;
                h1 += h2;

                h2 = (h2 << 41) | (h2 >>> 64 - 41);

                k2 *= c2;
                k2 = (k2 << 23) | (k2 >>> 64 - 23);
                k2 *= c1;
                h2 ^= k2;
                h2 += h1;

                h1 = h1 * 3 + 0x52dce729;
                h2 = h2 * 3 + 0x38495ab5;
            default:
        }

        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash3_fmix(h1);
        h2 = MurmurHash3_fmix(h2);

        return h1 + h2;
    }

    public static int MurmurHash3_fmix(int k) {
        k ^= k >>> 16;
        k *= 0x85ebca6b;
        k ^= k >>> 13;
        k *= 0xc2b2ae35;
        k ^= k >>> 16;
        return k;
    }

    public static long MurmurHash3_fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Hash function based on Knuth's multiplicative method. This version is faster than using Murmur hash but provides
     * acceptable behavior.
     *
     * @param k the long for which the hash will be calculated
     * @return the hash
     */
    public static long fastLongMix(long k) {
        // phi = 2^64 / goldenRatio
        final long phi = 0x9E3779B97F4A7C15L;
        long h = k * phi;
        h ^= h >>> 32;
        return h ^ (h >>> 16);
    }

    /**
     * Hash function based on Knuth's multiplicative method. This version is faster than using Murmur hash but provides
     * acceptable behavior.
     *
     * @param k the integer for which the hash will be calculated
     * @return the hash
     */
    public static int fastIntMix(int k) {
        // phi = 2^32 / goldenRatio
        final int phi = 0x9E3779B9;
        final int h = k * phi;
        return h ^ (h >>> 16);
    }

    /**
     * Hash code for multiple objects using {@link Arrays#hashCode(Object[])}.
     */
    public static int hashCode(Object... objects) {
        return Arrays.hashCode(objects);
    }

    /**
     * A function that calculates the index (e.g. to be used in an array/list) for a given hash. The returned value will always
     * be equal or larger than 0 and will always be smaller than 'length'.
     *
     * The reason this function exists is to deal correctly with negative and especially the Integer.MIN_VALUE; since that can't
     * be used safely with a Math.abs function.
     *
     * @param length the length of the array/list
     * @return the mod of the hash
     * @throws IllegalArgumentException if length is smaller than 1.
     */
    public static int hashToIndex(int hash, int length) {
        checkPositive("length", length);

        if (hash == Integer.MIN_VALUE) {
            return 0;
        }

        return abs(hash) % length;
    }

    /**
     * Computes the key perturbation value applied before hashing. The returned value
     * should be non-zero and different for each capacity. This matters because
     * keys are nearly ordered by hashcode so when adding one container's
     * values to the other, the number of collisions can skyrocket into the worst case
     * possible.
     * <p>
     * If it is known that hash containers will not be added to each other
     * (will be used for counting only, for example) then some speed can be gained by
     * not perturbing keys before hashing and returning a value of zero for all possible
     * capacities. The speed gain is a result of faster rehash operation (keys are mostly
     * in order).
     */
    public static int computePerturbationValue(int capacity) {
        return PERTURBATIONS[Integer.numberOfLeadingZeros(capacity)];
    }

    abstract static class LoadStrategy<R> implements ByteAccessStrategy<R> {

        abstract int getInt(R resource, long offset);

        abstract long getLong(R resource, long offset);

        @Override
        public final void putByte(R resource, long offset, byte value) {
        }
    }

    private static final class ByteArrayLoadStrategy extends LoadStrategy<byte[]> {

        @Override
        public int getInt(byte[] buf, long offset) {
            return EndiannessUtil.readIntL(this, buf, offset);
        }

        @Override
        public long getLong(byte[] buf, long offset) {
            return EndiannessUtil.readLongL(this, buf, offset);
        }

        @Override
        public byte getByte(byte[] buf, long offset) {
            return buf[(int) offset];
        }
    }

    private static final class WideDirectLoadStrategy extends LoadStrategy<MemoryAccessor> {

        @Override
        public int getInt(MemoryAccessor mem, long offset) {
            return mem.getInt(offset);
        }

        @Override
        public long getLong(MemoryAccessor mem, long offset) {
            return mem.getLong(offset);
        }

        @Override
        public byte getByte(MemoryAccessor mem, long offset) {
            return mem.getByte(offset);
        }
    }

    private static final class NarrowDirectLoadStrategy extends LoadStrategy<MemoryAccessor> {

        @Override
        public int getInt(MemoryAccessor mem, long offset) {
            return EndiannessUtil.readIntL(this, mem, offset);
        }

        @Override
        public long getLong(MemoryAccessor mem, long offset) {
            return EndiannessUtil.readLongL(this, mem, offset);
        }

        @Override
        public byte getByte(MemoryAccessor mem, long offset) {
            return mem.getByte(offset);
        }
    }
}
