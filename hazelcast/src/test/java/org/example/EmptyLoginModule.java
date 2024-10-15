/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package org.example;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import java.util.Map;

/**
 * Test-only, empty login module to use with configuration.
 */
public class EmptyLoginModule implements javax.security.auth.spi.LoginModule {
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
    }

    @Override
    public boolean login() {
        return false;
    }

    @Override
    public boolean commit() {
        return false;
    }

    @Override
    public boolean abort() {
        return false;
    }

    @Override
    public boolean logout() {
        return false;
    }
}
