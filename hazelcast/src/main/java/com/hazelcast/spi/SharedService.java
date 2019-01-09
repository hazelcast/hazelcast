/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * Marks a service which exposes its data (e.g. states) or behaviour to other services.
 * <p/>
 * If a service needs to share internal data or behaviour it has to implement this interface,
 * otherwise it will be an isolated service.
 * <p/>
 * Shared services are accessible via {@link NodeEngine#getSharedService(String)}.
 *
 * @deprecated since 3.7. A service can be retrieved using {@link NodeEngine#getService(String)}
 */
public interface SharedService {
}
