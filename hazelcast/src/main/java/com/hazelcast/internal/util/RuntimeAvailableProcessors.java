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

package com.hazelcast.internal.util;

/**
 * Utility class to access {@link Runtime#availableProcessors()} and optionally override its return value.
 * <p>
 * This class makes the number of available processors configurable for the sake of testing.
 * {@link #override(int)} and {@link #resetOverride()} should only be used for testing purposes.
 */
public final class RuntimeAvailableProcessors {

    // number of available processors currently configured
    private static volatile int currentAvailableProcessors = Runtime.getRuntime().availableProcessors();
    // number of processors to be used when reset
    private static volatile int defaultAvailableProcessors = Runtime.getRuntime().availableProcessors();

    private RuntimeAvailableProcessors() {
    }

    /**
     * Returns the number of available processors.
     * <p>
     * Returned value is either equal to {@link Runtime#availableProcessors()}
     * or an overridden value by call of the method {@link #override(int)}.
     *
     * @return number of available processors
     */
    public static int get() {
        return currentAvailableProcessors;
    }

    /**
     * Overrides the number of available processors returned by the method {@link #get()}.
     * <p>
     * This is to be used only for testing.
     *
     * @param availableProcessors number of available processors
     */
    public static void override(int availableProcessors) {
        RuntimeAvailableProcessors.currentAvailableProcessors = availableProcessors;
    }

    /**
     * Overrides the number of available processors that are set by the method {@link #override(int)}
     * <p>
     * This is to be used only for testing.
     *
     * @param availableProcessors
     */
    public static void overrideDefault(int availableProcessors) {
        defaultAvailableProcessors = availableProcessors;
    }

    /**
     * Resets the overridden number of available processors to {@link Runtime#availableProcessors()}.
     * <p>
     * This is to be used only for testing.
     */
    public static void resetOverride() {
        RuntimeAvailableProcessors.currentAvailableProcessors = defaultAvailableProcessors;
    }
}
