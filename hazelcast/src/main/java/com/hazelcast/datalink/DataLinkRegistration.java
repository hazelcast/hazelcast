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

package com.hazelcast.datalink;

import com.hazelcast.spi.annotation.Beta;

/**
 * Registration for a {@link DataLink}.
 *
 * @since 5.3
 */
@Beta
public interface DataLinkRegistration {

    /**
     * Mapping Type of registered data link.
     */
    String type();

    /**
     *
     * @return class name of registered data link.
     */
    Class<? extends DataLink> clazz();

}
