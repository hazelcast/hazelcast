/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.nio.ByteOrder;
import java.util.Arrays;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"SF_SWITCH_FALLTHROUGH", "SF_SWITCH_NO_DEFAULT"})
public final class HashUtil {

    private static final boolean LITTLE_ENDIAN = ByteOrder.LITTLE_ENDIAN == ByteOrder.nativeOrder();
    private static final int DEFAULT_MURMUR_SEED = 0x01000193;

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
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
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
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
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

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        Unsafe unsafe = UnsafeHelper.UNSAFE;
        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = LITTLE_ENDIAN ? unsafe.getInt(address + i)
                    : (unsafe.getByte(address + i + 3) & 0xff)
                        | ((unsafe.getByte(address + i + 2) & 0xff) << 8)
                        | ((unsafe.getByte(address + i + 1) & 0xff) << 16)
                        | (unsafe.getByte(address + i) << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (unsafe.getByte(address + roundedEnd + 2) & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (unsafe.getByte(address + roundedEnd + 1) & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= unsafe.getByte(address + roundedEnd) & 0xff;
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
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
            case 15: k2 ^= (long) data[tail + 14] << 48;
            case 14: k2 ^= (long) data[tail + 13] << 40;
            case 13: k2 ^= (long) data[tail + 12] << 32;
            case 12: k2 ^= (long) data[tail + 11] << 24;
            case 11: k2 ^= (long) data[tail + 10] << 16;
            case 10: k2 ^= (long) data[tail + 9] << 8;
            case 9:  k2 ^= data[tail + 8];

            case 8:  k1 ^= (long) data[tail + 7] << 56;
            case 7:  k1 ^= (long) data[tail + 6] << 48;
            case 6:  k1 ^= (long) data[tail + 5] << 40;
            case 5:  k1 ^= (long) data[tail + 4] << 32;
            case 4:  k1 ^= (long) data[tail + 3] << 24;
            case 3:  k1 ^= (long) data[tail + 2] << 16;
            case 2:  k1 ^= (long) data[tail + 1] << 8;
            case 1:  k1 ^= data[tail];

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

//                c1 = c1 * 5 + 0x7b7d159c;   // unused, used only for 128-bit version
//                c2 = c2 * 5 + 0x6bce6396;   // unused, used only for 128-bit version
        }

        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash3_fmix(h1);
        h2 = MurmurHash3_fmix(h2);

        h1 += h2;
//        h2 += h1; // unused, used only for 128-bit version
        return h1;
    }

    public static long MurmurHash3_x64_64_direct(long address, int offset, int len) {
        return MurmurHash3_x64_64_direct(address, offset, len, DEFAULT_MURMUR_SEED);
    }

    public static long MurmurHash3_x64_64_direct(long address, int offset, int len, final int seed) {
        long h1 = 0x9368e53c2f6af274L ^ seed;
        long h2 = 0x586dcd208f7cd3fdL ^ seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        long k1 = 0;
        long k2 = 0;

        for (int i = 0; i < len / 16; i++) {
            k1 = MurmurHash3_getBlock_direct(address, (i * 2 * 8) + offset);
            k2 = MurmurHash3_getBlock_direct(address, ((i * 2 + 1) * 8) + offset);

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
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        switch (len & 15) {
            case 15: k2 ^= (long) unsafe.getByte(address + tail + 14) << 48;
            case 14: k2 ^= (long) unsafe.getByte(address + tail + 13) << 40;
            case 13: k2 ^= (long) unsafe.getByte(address + tail + 12) << 32;
            case 12: k2 ^= (long) unsafe.getByte(address + tail + 11) << 24;
            case 11: k2 ^= (long) unsafe.getByte(address + tail + 10) << 16;
            case 10: k2 ^= (long) unsafe.getByte(address + tail + 9) << 8;
            case 9:  k2 ^= unsafe.getByte(address + tail + 8);

            case 8:  k1 ^= (long) unsafe.getByte(address + tail + 7) << 56;
            case 7:  k1 ^= (long) unsafe.getByte(address + tail + 6) << 48;
            case 6:  k1 ^= (long) unsafe.getByte(address + tail + 5) << 40;
            case 5:  k1 ^= (long) unsafe.getByte(address + tail + 4) << 32;
            case 4:  k1 ^= (long) unsafe.getByte(address + tail + 3) << 24;
            case 3:  k1 ^= (long) unsafe.getByte(address + tail + 2) << 16;
            case 2:  k1 ^= (long) unsafe.getByte(address + tail + 1) << 8;
            case 1:  k1 ^= unsafe.getByte(address + tail);

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

//                c1 = c1 * 5 + 0x7b7d159c;   // unused, used only for 128-bit version
//                c2 = c2 * 5 + 0x6bce6396;   // unused, used only for 128-bit version
        }

        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash3_fmix(h1);
        h2 = MurmurHash3_fmix(h2);

        h1 += h2;
//        h2 += h1; // unused, used only for 128-bit version
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

    private static long MurmurHash3_getBlock_direct(long address, int i) {
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        if (LITTLE_ENDIAN) {
            return unsafe.getLong(address + i);
        } else {
            return ((unsafe.getByte(address + i) & 0x00000000000000FFL))
                    | ((unsafe.getByte(address + i + 1) & 0x00000000000000FFL) << 8)
                    | ((unsafe.getByte(address + i + 2) & 0x00000000000000FFL) << 16)
                    | ((unsafe.getByte(address + i + 3) & 0x00000000000000FFL) << 24)
                    | ((unsafe.getByte(address + i + 4) & 0x00000000000000FFL) << 32)
                    | ((unsafe.getByte(address + i + 5) & 0x00000000000000FFL) << 40)
                    | ((unsafe.getByte(address + i + 6) & 0x00000000000000FFL) << 48)
                    | ((unsafe.getByte(address + i + 7) & 0x00000000000000FFL) << 56);
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

    /**
     * Hash code for multiple objects using {@link Arrays#hashCode(Object[])}.
     **/
    public static int hashCode(Object ... objects) {
        return Arrays.hashCode(objects);
    }

    private HashUtil(){}
}
