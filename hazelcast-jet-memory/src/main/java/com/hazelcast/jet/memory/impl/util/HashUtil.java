package com.hazelcast.jet.memory.impl.util;

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.memory.MemoryAccessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteOrder;
import java.util.Arrays;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.abs;

/**
 * Utility methods related to hashtables.
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

    private static final boolean LITTLE_ENDIAN = ByteOrder.LITTLE_ENDIAN == ByteOrder.nativeOrder();
    private static final int DEFAULT_MURMUR_SEED = 0x01000193;
    private static final int[] PERTURBATIONS = new int[Integer.SIZE];

    static {
        final int primeDisplacement = 17;
        for (int i = 0; i < PERTURBATIONS.length; i++) {
            PERTURBATIONS[i] = MurmurHash3_fmix(primeDisplacement + i);
        }
    }

    private HashUtil() {
    }

    public static int MurmurHash3_x86_32(byte[] data, int offset, int len) {
        return MurmurHash3_x86_32(data, offset, len, DEFAULT_MURMUR_SEED);
    }

    /**
     * Returns the MurmurHash3_x86_32 hash.
     */
    public static int MurmurHash3_x86_32(byte[] data, int offset, int len, int seed) {
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        // round down to 4 byte block
        int roundedEnd = offset + (len & 0xfffffffc);

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);
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
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= data[roundedEnd] & 0xff;
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

    public static int MurmurHash3_x86_32_direct(long address, int offset, int len) {
        return MurmurHash3_x86_32_direct(address, offset, len, DEFAULT_MURMUR_SEED);
    }

    /**
     * Returns the MurmurHash3_x86_32 hash.
     */
    public static int MurmurHash3_x86_32_direct(long address, int offset, int len, int seed) {
        return MurmurHash3_x86_32_direct(MEM, address, offset, len, seed, !LITTLE_ENDIAN);
    }

    /**
     * Returns the MurmurHash3_x86_32 hash.
     */
    public static int MurmurHash3_x86_32_direct(MemoryAccessor memoryAccessor, long address,
                                                int offset, int len, int seed, boolean useBigEndian) {
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        // round down to 4 byte block
        int roundedEnd = offset + (len & 0xfffffffc);

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = !useBigEndian ? memoryAccessor.getInt(address + i)
                    : (memoryAccessor.getByte(address + i) & 0xff)
                    | ((memoryAccessor.getByte(address + i + 1) & 0xff) << 8)
                    | ((memoryAccessor.getByte(address + i + 2) & 0xff) << 16)
                    | (memoryAccessor.getByte(address + i + 3) << 24);
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
                k1 = (memoryAccessor.getByte(address + roundedEnd + 2) & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (memoryAccessor.getByte(address + roundedEnd + 1) & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= memoryAccessor.getByte(address + roundedEnd) & 0xff;
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

    public static long MurmurHash3_x64_64(final byte[] data, int offset, int len) {
        return MurmurHash3_x64_64(data, offset, len, DEFAULT_MURMUR_SEED);
    }

    public static long MurmurHash3_x64_64(final byte[] data, int offset, int len, final int seed) {
        long h1 = 0x9368e53c2f6af274L ^ seed;
        long h2 = 0x586dcd208f7cd3fdL ^ seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        long k1 = 0;
        long k2 = 0;

        for (int i = 0; i < len / 16; i++) {
            k1 = MurmurHash3_getBlock(data, (i * 2 * 8) + offset);
            k2 = MurmurHash3_getBlock(data, ((i * 2 + 1) * 8) + offset);

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

        int tail = ((len >>> 4) << 4) + offset;

        switch (len & 15) {
            case 15:
                k2 ^= (long) data[tail + 14] << 48;
            case 14:
                k2 ^= (long) data[tail + 13] << 40;
            case 13:
                k2 ^= (long) data[tail + 12] << 32;
            case 12:
                k2 ^= (long) data[tail + 11] << 24;
            case 11:
                k2 ^= (long) data[tail + 10] << 16;
            case 10:
                k2 ^= (long) data[tail + 9] << 8;
            case 9:
                k2 ^= data[tail + 8];

            case 8:
                k1 ^= (long) data[tail + 7] << 56;
            case 7:
                k1 ^= (long) data[tail + 6] << 48;
            case 6:
                k1 ^= (long) data[tail + 5] << 40;
            case 5:
                k1 ^= (long) data[tail + 4] << 32;
            case 4:
                k1 ^= (long) data[tail + 3] << 24;
            case 3:
                k1 ^= (long) data[tail + 2] << 16;
            case 2:
                k1 ^= (long) data[tail + 1] << 8;
            case 1:
                k1 ^= data[tail];

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

                //c1 = c1 * 5 + 0x7b7d159c;   // unused, used only for 128-bit version
                //c2 = c2 * 5 + 0x6bce6396;   // unused, used only for 128-bit version
            default:
        }

        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash3_fmix(h1);
        h2 = MurmurHash3_fmix(h2);

        h1 += h2;
        //h2 += h1; // unused, used only for 128-bit version
        return h1;
    }

    public static long MurmurHash3_x64_64_direct(long address, int offset, int len, final int seed) {
        return MurmurHash3_x64_64_direct(MEM, address, offset, len, seed, !LITTLE_ENDIAN);
    }


    public static long MurmurHash3_x64_64_direct(long address, int offset, int len) {
        return MurmurHash3_x64_64_direct(MEM, address, offset, len, DEFAULT_MURMUR_SEED, !LITTLE_ENDIAN);
    }

    public static long MurmurHash3_x64_64_direct(MemoryAccessor memoryAccessor, long address, int offset, int len,
                                                 final int seed, boolean useBigEndian) {
        long h1 = 0x9368e53c2f6af274L ^ seed;
        long h2 = 0x586dcd208f7cd3fdL ^ seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        long k1 = 0;
        long k2 = 0;

        for (int i = 0; i < len / 16; i++) {
            k1 = MurmurHash3_getBlock_direct(memoryAccessor, address, (i * 2 * 8) + offset, useBigEndian);
            k2 = MurmurHash3_getBlock_direct(memoryAccessor, address, ((i * 2 + 1) * 8) + offset, useBigEndian);

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

        int tail = ((len >>> 4) << 4) + offset;
        switch (len & 15) {
            case 15:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 14) << 48;
            case 14:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 13) << 40;
            case 13:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 12) << 32;
            case 12:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 11) << 24;
            case 11:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 10) << 16;
            case 10:
                k2 ^= (long) memoryAccessor.getByte(address + tail + 9) << 8;
            case 9:
                k2 ^= memoryAccessor.getByte(address + tail + 8);

            case 8:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 7) << 56;
            case 7:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 6) << 48;
            case 6:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 5) << 40;
            case 5:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 4) << 32;
            case 4:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 3) << 24;
            case 3:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 2) << 16;
            case 2:
                k1 ^= (long) memoryAccessor.getByte(address + tail + 1) << 8;
            case 1:
                k1 ^= memoryAccessor.getByte(address + tail);

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

                //c1 = c1 * 5 + 0x7b7d159c;   // unused, used only for 128-bit version
                //c2 = c2 * 5 + 0x6bce6396;   // unused, used only for 128-bit version
            default:
        }

        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash3_fmix(h1);
        h2 = MurmurHash3_fmix(h2);

        h1 += h2;
        //h2 += h1; // unused, used only for 128-bit version
        return h1;
    }

    private static long MurmurHash3_getBlock(byte[] key, int i) {
        return ((key[i] & 0x00000000000000FFL))
                | ((key[i + 1] & 0x00000000000000FFL) << 8)
                | ((key[i + 2] & 0x00000000000000FFL) << 16)
                | ((key[i + 3] & 0x00000000000000FFL) << 24)
                | ((key[i + 4] & 0x00000000000000FFL) << 32)
                | ((key[i + 5] & 0x00000000000000FFL) << 40)
                | ((key[i + 6] & 0x00000000000000FFL) << 48)
                | ((key[i + 7] & 0x00000000000000FFL) << 56);
    }

    private static long MurmurHash3_getBlock_direct(MemoryAccessor memoryAccessor, long address, int i, boolean useBigEndian) {
        if (!useBigEndian) {
            return memoryAccessor.getLong(address + i);
        } else {
            return ((memoryAccessor.getByte(address + i) & 0x00000000000000FFL))
                    | ((memoryAccessor.getByte(address + i + 1) & 0x00000000000000FFL) << 8)
                    | ((memoryAccessor.getByte(address + i + 2) & 0x00000000000000FFL) << 16)
                    | ((memoryAccessor.getByte(address + i + 3) & 0x00000000000000FFL) << 24)
                    | ((memoryAccessor.getByte(address + i + 4) & 0x00000000000000FFL) << 32)
                    | ((memoryAccessor.getByte(address + i + 5) & 0x00000000000000FFL) << 40)
                    | ((memoryAccessor.getByte(address + i + 6) & 0x00000000000000FFL) << 48)
                    | ((memoryAccessor.getByte(address + i + 7) & 0x00000000000000FFL) << 56);
        }
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

    public static long fastLongMix(long k) {
        // phi = 2^64 / goldenRatio
        final long phi = 0x9E3779B97F4A7C15L;
        long h = k * phi;
        h ^= h >>> 32;
        return h ^ (h >>> 16);
    }

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
     * <p>
     * The reason this function exists is to deal correctly with negative and especially the Integer.MIN_VALUE; since that can't
     * be used safely with a Math.abs function.
     *
     * @param length the length of the array/list
     * @return the mod of the hash
     * @throws IllegalArgumentException if mod smaller than 1.
     */
    public static int hashToIndex(int hash, int length) {
        checkPositive(length, "mod must be larger than 0");

        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        } else {
            hash = abs(hash);
        }

        return hash % length;
    }

    /**
     * <p>Compute the key perturbation value applied before hashing. The returned value
     * should be non-zero and ideally different for each capacity. This matters because
     * keys are nearly-ordered by their hashed values so when adding one container's
     * values to the other, the number of collisions can skyrocket into the worst case
     * possible.
     * <p>
     * <p>If it is known that hash containers will not be added to each other
     * (will be used for counting only, for example) then some speed can be gained by
     * not perturbing keys before hashing and returning a value of zero for all possible
     * capacities. The speed gain is a result of faster rehash operation (keys are mostly
     * in order).
     */
    public static int computePerturbationValue(int capacity) {
        return PERTURBATIONS[Integer.numberOfLeadingZeros(capacity)];
    }
}
