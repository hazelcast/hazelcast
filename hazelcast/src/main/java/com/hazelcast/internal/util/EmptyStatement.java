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
 * This class makes CheckStyle happy, if you have an ignored or expected exception,
 * but need at least a single statement in the {@code catch} clause.
 */
public final class EmptyStatement {

    private EmptyStatement() {
    }

    /**
     * Ignores the given exception.
     *
     * @param t the exception to ignore
     */
    public static void ignore(Throwable t) {
    }
}
