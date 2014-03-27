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

package com.hazelcast.spi;

/**
 * An Marker interface that signals that an operation is an urgent System Operation.
 * <p/>
 * System Operations can be executed by the {@link com.hazelcast.spi.OperationService} with an urgency. This
 * is important because when a system is under load and its operation queues are filled up, you want the system to
 * deal with system operation like the ones needed for partition-migration, with a high urgency. So that system
 * remains responsive.
 * <p/>
 * In most cases this interface should not be used by normal user code because illegal usage of this interface,
 * can influence the health of the Hazelcast cluster negatively.
 */
public interface UrgentSystemOperation {
}
