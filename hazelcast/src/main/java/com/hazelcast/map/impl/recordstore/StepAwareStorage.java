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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.operation.steps.engine.Step;

import java.util.function.Consumer;

/**
 * Interface for a storage-backend (e.g. B+tree or a
 * record store) that supports Steps infrastructure
 * and injects a Step to an IMap operation.
 */
public interface StepAwareStorage {

    /**
     * Appends provided step to the head of step-chain.
     * @param steps consumer which adds step to the head of step-chain.
     */
    void addAsHeadStep(Consumer<Step> steps);

}
