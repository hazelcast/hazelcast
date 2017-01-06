/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading;

import cascading.tuple.Hasher;

import java.io.Serializable;
import java.util.Comparator;

public class TestStringComparator implements Hasher<String>, Comparator<String>, Serializable {
    boolean reverse = true;

    public TestStringComparator() {
    }

    public TestStringComparator(boolean reverse) {
        this.reverse = reverse;
    }

    @Override
    public int compare(String o1, String o2) {
        return reverse ? o2.compareTo(o1) : o1.compareTo(o2);
    }

    @Override
    public int hashCode(String value) {
        if (value == null)
            return 0;
        return value.hashCode();
    }
}
