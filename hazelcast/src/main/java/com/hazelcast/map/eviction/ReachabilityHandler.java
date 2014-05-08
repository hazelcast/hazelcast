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

package com.hazelcast.map.eviction;

/**
 * @param <T> Type of object which will be checked for reachable.
 */
public interface ReachabilityHandler<T> {
    /**
     * @param record
     * @param criteria
     * @param time
     * @return <tt>null</tt> if record is not reachable atm.
     * otherwise returns record. <tt>T</tt>
     */
    T process(T record, long criteria, long time);

    /**
     * @return Handler priority.
     * Min nice values correspond to a higher priority.
     */
    short niceNumber();

    void setSuccessorHandler(ReachabilityHandler successorHandler);

    ReachabilityHandler getSuccessorHandler();

    void resetHandler();

}
