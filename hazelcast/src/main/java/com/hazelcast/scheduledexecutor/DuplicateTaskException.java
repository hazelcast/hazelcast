/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.serialization.BinaryInterface;

import static com.hazelcast.nio.serialization.BinaryInterface.Reason.OTHER_CONVENTION;

/**
 * An exception thrown when a task's name is already used before for another (or the same, if re-attempted) schedule.
 * Tasks under a scheduler must have unique names.
 */
@BinaryInterface(reason = OTHER_CONVENTION)
public class DuplicateTaskException
        extends HazelcastException {

    public DuplicateTaskException(String msg) {
        super(msg);
    }

}
