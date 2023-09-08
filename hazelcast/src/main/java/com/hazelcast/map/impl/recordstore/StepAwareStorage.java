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

import java.util.List;

/**
 * Interface for a storage-backend (e.g. B+tree or a record store)
 * that supports Steps infrastructure and injects Steps on IMap operation.
 */
public interface StepAwareStorage {

    /**
     * Appends extra steps to the head of an operation's
     * existing step chain in the provided order.
     * <p>
     * So after append, your 1st step from this new list
     * will be the first step of the existing step chain,
     * 2nd will be the 2nd and so on.
     *
     * @return list of steps to be appended to head of existing step chain
     */
    List<Step> headSteps();

}
