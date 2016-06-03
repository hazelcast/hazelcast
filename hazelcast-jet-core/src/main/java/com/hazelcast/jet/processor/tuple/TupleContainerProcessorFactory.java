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

package com.hazelcast.jet.processor.tuple;

import com.hazelcast.jet.processor.ContainerProcessorFactory;
import com.hazelcast.jet.data.tuple.JetTuple;

/**
 * Factory to create JET-processors which work with tuples;
 *
 * @param <KeyInput>    - type of input tuple key part;
 * @param <ValueInput>  - type of input tuple value part;
 * @param <KeyOutPut>   - type of output tuple  key part;
 * @param <ValueOutPut> - type of output tuple  value part;
 */
public interface TupleContainerProcessorFactory<KeyInput, ValueInput, KeyOutPut, ValueOutPut>
        extends ContainerProcessorFactory<JetTuple<KeyInput, ValueInput>, JetTuple<KeyOutPut, ValueOutPut>> {

}
