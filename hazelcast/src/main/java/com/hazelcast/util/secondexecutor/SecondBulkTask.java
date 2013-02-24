/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.secondexecutor;

import java.util.concurrent.ConcurrentMap;

interface SecondBulkTask extends SecondTask {
    /**
     * Executes all entries in one shot. Implementation has to
     * handle the failures and can possibly reschedule it for a future time.
     * Imagine you are implementing this for a dirty records. If mapStore.storeAll
     * throws exception, you might want to reschedule the failed records.
     *
     * @param ses
     * @param entries
     * @param delaySecond delaySeconds set for this entry
     */
    void executeAll(SecondExecutorService ses, ConcurrentMap<Object, Object> entries, int delaySecond);
}
