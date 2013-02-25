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

import java.util.Map;

interface SecondEntryTask extends SecondTask {

    /**
     * Executes a single entry. All failures has to be handled by the implementation.
     * Implementation can choose to ignore the failures or reschedules the entry for
     * a future time.
     *
     * @param ses
     * @param entry
     * @param delaySeconds delaySeconds set for this entry
     */
    void executeEntry(SecondExecutorService ses, Map.Entry entry, int delaySeconds);
}
