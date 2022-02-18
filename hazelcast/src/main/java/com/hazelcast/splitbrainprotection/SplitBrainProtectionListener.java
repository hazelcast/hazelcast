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

package com.hazelcast.splitbrainprotection;

import java.util.EventListener;

/**
 * Listener to get notified when a split brain protection state is changed.
 * <p>
 * {@code SplitBrainProtectionEvent}s are fired only after the minimum cluster size
 * requirement is met for the first time.
 * For instance, see the following scenario for a minimum cluster size is equal to 3:
 * <ul>
 * <li>Member-1 starts; no events are fired, since cluster size is still below minimum size.</li>
 * <li>Member-2 starts; no events are fired, since cluster size is still below minimum size.</li>
 * <li>Member-3 starts; no events yet, since this is the first time minimum cluster size is reached.</li>
 * <li>Member-1 stops; both Member-2 and Member-3 fire splitbrain protection missing events,
 * since member count drops below 3.</li>
 * <li>Member-1 restarts; both Member-2 and Member-3 fire splitbrain protection present events,
 * but Member-1 does not, because for Member-1 this is the first time minimum cluster size is met.</li>
 * </ul>
 */
@FunctionalInterface
public interface SplitBrainProtectionListener extends EventListener {

    /**
     * Called when the state of the split brain detector is changed.
     *
     * @param splitBrainProtectionEvent provides information about split brain protection
     *        presence and current member list.
     */
    void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent);

}
