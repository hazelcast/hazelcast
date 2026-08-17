/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

import javax.annotation.Nonnull;

import static java.lang.Integer.getInteger;

public class GlobalJUnitReconfigurationSessionListener implements LauncherSessionListener {

    private static final int DEFAULT_TEST_TIMEOUT_IN_SECONDS =
            getInteger("hazelcast.test.defaultTestTimeoutInSeconds", 300);

    /**
     * Sets the default timeout according to our internal setting.
     */
    @Override
    public void launcherSessionOpened(@Nonnull LauncherSession session) {
        System.setProperty("junit.jupiter.execution.timeout.default", DEFAULT_TEST_TIMEOUT_IN_SECONDS + "s");
    }
}
