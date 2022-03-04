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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown from member if that member is not able to generate IDs using Flake ID generator
 * because its node ID is too big.
 */
public class NodeIdOutOfRangeException extends HazelcastException {
    public NodeIdOutOfRangeException(String message) {
        super(message);
    }
}
