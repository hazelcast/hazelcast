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

package com.hazelcast.collection.impl.queue.model;

import java.util.Comparator;

public class PriorityElementComparator implements Comparator<PriorityElement> {
    @Override
    public int compare(PriorityElement o1, PriorityElement o2) {
        if (o1.isHighPriority() && !o2.isHighPriority()) {
            return -1;
        }
        if (o2.isHighPriority() && !o1.isHighPriority()) {
            return 1;
        }
        return Integer.compare(o1.getVersion(), o2.getVersion());
    }
}
