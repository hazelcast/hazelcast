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

package com.hazelcast.table.impl;


import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.alto.OpCodes;

import java.util.concurrent.atomic.AtomicLong;

public final class NoOp extends Op {

    public final static AtomicLong counter = new AtomicLong();

    public NoOp() {
        super(OpCodes.NOOP);
    }

    @Override
    public int run() throws Exception {
        return COMPLETED;
    }
}
