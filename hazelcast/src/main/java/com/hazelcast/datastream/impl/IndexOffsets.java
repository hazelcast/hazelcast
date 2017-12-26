/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static java.lang.Math.abs;

public final class IndexOffsets {

    private IndexOffsets() {
    }

    public static int offsetInIndex(boolean b) {
        return b ? 0 : 1;
    }

    public static int offsetInIndex(byte b) {
        return 128 + b;
    }

    public static int offsetInIndex(char c, int indexSizeDiv4) {
        return modPowerOfTwo(abs(c), indexSizeDiv4);
    }

    public static int offsetInIndex(short s, int indexSizeDiv4) {
        return modPowerOfTwo(abs(s), indexSizeDiv4);
    }

    public static int offsetInIndex(int i, int indexSizeDiv4) {
        return modPowerOfTwo(abs(i), indexSizeDiv4);
    }

    public static int offsetInIndex(long l, int indexSizeDiv4) {
        return (int) modPowerOfTwo(abs(l), indexSizeDiv4);
    }

    public static int offsetInIndex(float f, int indexSizeDiv4) {
        return modPowerOfTwo(abs(Float.floatToIntBits(f)), indexSizeDiv4);
    }

    public static int offsetInIndex(double d, int indexSizeDiv4) {
        return (int) modPowerOfTwo(abs(Double.doubleToLongBits(d)), indexSizeDiv4);
    }

}
