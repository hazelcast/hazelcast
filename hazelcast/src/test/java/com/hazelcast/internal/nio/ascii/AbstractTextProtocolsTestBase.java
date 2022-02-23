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

package com.hazelcast.internal.nio.ascii;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.hazelcast.config.RestApiConfig;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestAwareInstanceFactory;

/**
 * Shared code for {@link RestApiConfig} and {@link TextProtocolFilter} testing.
 */
public abstract class AbstractTextProtocolsTestBase {

    public static final String CRLF = "\r\n";

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    protected AssertTask createResponseAssertTask(final String message, final TextProtocolClient client, final String expectedSubstring) {
        return new AssertTask() {
            @Override
            public void run() throws Exception {
                assertThat(message, client.getReceivedString(), containsString(expectedSubstring));
            }
        };
    }
}
