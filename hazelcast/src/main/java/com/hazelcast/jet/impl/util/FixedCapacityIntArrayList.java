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

package com.hazelcast.jet.impl.util;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Simple list of integers that stores its content in a non-resizeable array.
 * Simplified variant of {@link com.hazelcast.internal.util.collection.FixedCapacityArrayList}
 * for integers.
 */
public class FixedCapacityIntArrayList {
    private int[] elements;
    private int size;

    public FixedCapacityIntArrayList(int capacity) {
        elements = new int[capacity];
    }

    public void add(int element) {
        elements[size++] = element;
    }

    public int[] asArray() {
        int[] result = size == elements.length ? elements : Arrays.copyOfRange(elements, 0, size);
        elements = null;
        return result;
    }

    public IntStream stream() {
        return Arrays.stream(elements, 0, size);
    }
}
