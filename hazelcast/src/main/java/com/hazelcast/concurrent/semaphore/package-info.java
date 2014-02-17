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

/**
 * <p>This package contains ISemaphore functionality for Hazelcast.<br/>
 * The ISemaphore is the distributed version of the {@link java.util.concurrent.Semaphore}.
 *
 * Unfortunately the j.u.c.Semaphore doesn't expose an interface, so the ISemaphore copies most of the
 * method and unlike the {@link java.util.concurrent.locks.Lock} the {@link com.hazelcast.core.ISemaphore}
 * is not a drop in replacement for the {@link java.util.concurrent.Semaphore}.
 *
 * @since 3.2
 */
package com.hazelcast.concurrent.semaphore;
