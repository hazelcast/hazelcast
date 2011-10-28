/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.Instance;
import com.hazelcast.impl.monitor.SemaphoreOperationsCounter;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;

public interface SemaphoreProxy extends ISemaphore, Instance, HazelcastInstanceAwareInstance {
    public final static Data DATA_TRUE = IOUtil.toData(true);

    public final static int ACQUIRED = 0;
    public final static int ACQUIRE_FAILED = 1;
    public final static int INSTANCE_DESTROYED = 2;

    SemaphoreOperationsCounter getOperationsCounter();

    String getLongName();
}
