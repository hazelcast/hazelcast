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
 * An interface that can be implemented by a SPI service that needs to be notified members joining and leaving
 * the cluster.
 *
 * @author mdogan 9/5/12
 */
public interface MembershipAwareService {

    /**
     * Invoked when a new member is added to the cluster.
     */
    void memberAdded(MembershipServiceEvent event);

    /**
     * Invoked when an existing member leaves the cluster.
     */
    void memberRemoved(MembershipServiceEvent event);


    /**
     * Invoked when a member attribute is changed.
     */
    void memberAttributeChanged(MemberAttributeServiceEvent event);

}
