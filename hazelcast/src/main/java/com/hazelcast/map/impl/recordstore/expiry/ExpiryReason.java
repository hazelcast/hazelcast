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

package com.hazelcast.map.impl.recordstore.expiry;

/**
 * Represents expiry status of a key.
 *
 * There are 3 possible status:
 * <ul>
 *      <li>
 *          {@link ExpiryReason#TTL}: Time-to-live
 *          seconds has passed and key is not reachable.
 *      </li>
 *      <li>
 *          {@link ExpiryReason#MAX_IDLE_SECONDS}: Max-idle
 *          seconds has passed and key is not reachable.
 *      </li>
 *      <li>
 *          {@link ExpiryReason#NOT_EXPIRED}: Key is reachable.
 *      </li>
 * </ul>
 */
public enum ExpiryReason {
    TTL,
    MAX_IDLE_SECONDS,
    NOT_EXPIRED
}
