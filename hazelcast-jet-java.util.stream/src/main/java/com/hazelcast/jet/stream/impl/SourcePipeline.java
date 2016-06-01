/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.jet.spi.dag.tap.SourceTap;
import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;

public interface SourcePipeline<E_OUT> extends Pipeline<E_OUT> {
    /**
     * @return A function which will convert the output of this source from a Tuple to E_OUT
     */
    Distributed.Function<Tuple, E_OUT> fromTupleMapper();

    SourceTap getSourceTap();
}
