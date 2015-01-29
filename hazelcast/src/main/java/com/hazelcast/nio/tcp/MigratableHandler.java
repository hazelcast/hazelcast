/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

/**
 * Read/Write handlers that supports migration between {@link com.hazelcast.nio.tcp.IOSelector}.
 *
 */
public interface MigratableHandler {

    /**
     * Migrate to a new IOSelector.
     *
     * @param newOwner
     */
    void migrate(final IOSelector newOwner);

    /**
     * Get current IOSelector owning this handler
     *
     * @return current owner
     */
    IOSelector getOwner();
}
