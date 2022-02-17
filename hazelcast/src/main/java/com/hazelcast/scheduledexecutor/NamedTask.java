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

package com.hazelcast.scheduledexecutor;

/**
 * The <code>NamedTask</code> interface should be implemented by any task that needs to have an identification and enhanced
 * atomicity upon scheduling the task.
 */
@FunctionalInterface
public interface NamedTask {

    /**
     * Returns the name of the task. The name will be used to uniquely identify the task in the scheduler service, if a duplicate
     * is detected a {@link DuplicateTaskException} will be thrown and the task will be rejected.
     *
     * @return String The name of the task
     */
    String getName();

}
