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

package com.hazelcast.cluster;

/**
 * This is a super nasty hack to make sure that the LogEvent can be moved to
 * this module. The LogEvent relies on this interface, so here we have a fake
 * interface so that the LogEvent can compile. In the main hazelcast module
 * there is the proper implementation of the Member and in the final jar that
 * class file will be present and not this one.
 * <p/>
 * As soon as the the {@link com.hazelcast.logging.LogEvent} is removed, this
 * interface can be removed.
 */
public interface Member {
}
