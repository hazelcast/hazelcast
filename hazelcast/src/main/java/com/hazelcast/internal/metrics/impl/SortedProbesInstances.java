/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

/**
 * A list that contains probes. The probes are sorted based on their names.
 *
 * It also contains the modCount so that the system can determine when the SortedProbeInstance needs to be refreshed.
 *
 * It can be that the SortedProbeInstance contains ProbeInstance that are created after the modCount is increased
 * (so the modCount) could be to old. When this happens a subsequent call to render will get the SortedProbeInstances
 * refreshed; now with a newer modcount (and potentially the same probes).
 *
 * This structure is effectively immutable. After it is created, it won't change.
 */
class SortedProbesInstances extends ArrayList<ProbeInstance> {

    static final Comparator<ProbeInstance> COMPARATOR = new Comparator<ProbeInstance>() {
        @Override
        public int compare(ProbeInstance o1, ProbeInstance o2) {
            return o1.name.compareTo(o2.name);
        }
    };

    final int modCount;

    SortedProbesInstances() {
        this.modCount = 0;
    }

    SortedProbesInstances(Collection<ProbeInstance> values, int modCount) {
        super(values);
        this.modCount = modCount;

        Collections.sort(this, COMPARATOR);
    }
}
