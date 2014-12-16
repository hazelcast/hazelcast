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

package com.hazelcast.core;

/**
 * <p>Implementations of this interface select members
 * that are capable of executing a special kind of task.<br/>
 * The {@link #select(Member)} method is called for every available
 * member in the cluster and it is up to the implementation to decide
 * if the member is going to be used or not.</p>
 * <p>For example, a basic implementation could select members on the
 * existence of a special attribute in the members, like the following
 * example:<br/>
 * <pre>public class MyMemberSelector implements MemberSelector {
 *     public boolean select(Member member) {
 *         return Boolean.TRUE.equals(member.getAttribute("my.special.executor"));
 *     }
 * }</pre>
 * </p>
 */
public interface MemberSelector {

    /**
     * Decides if the given member will be part of an operation or not.
     *
     * @param member the member instance to decide upon
     * @return true if the member should take part in the operation, false otherwise
     */
    boolean select(Member member);

}
