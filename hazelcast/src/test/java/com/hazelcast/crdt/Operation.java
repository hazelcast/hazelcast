/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.crdt;

import java.util.List;
import java.util.Random;

public class Operation<T, S> {

    protected final Random rnd;
    protected final int crdtIndex;

    public Operation(Random rnd) {
        this.rnd = rnd;
        this.crdtIndex = rnd.nextInt(10000);
    }

    protected T getCRDT(List<T> crdts) {
        return crdts.get(crdtIndex % crdts.size());
    }

    protected void perform(List<T> crdts, S state) {
        perform(getCRDT(crdts), state);
    }

    protected void perform(T crdt, S state) {
        // intended for override
    }
}
