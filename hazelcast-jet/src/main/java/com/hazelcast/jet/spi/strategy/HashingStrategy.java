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

package com.hazelcast.jet.spi.strategy;

import com.hazelcast.jet.spi.container.ContainerDescriptor;

import java.io.Serializable;

/**
 * Strategy to calculate hash for data passed between JET containers;
 * <p/>
 * Used for example to determine correct task which should consume data;
 * <p/>
 * For example object can be a tuple, partitionKey - fields to be used for hash calculation;
 *
 * @param <O> - type of the input object;
 * @param <K> - type of the partitionKey;
 */
public interface HashingStrategy<O, K> extends Serializable {
    int hash(O object, K partitionKey, ContainerDescriptor containerDescriptor);
}
