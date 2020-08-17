/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Alternative rule for changing Log4j2 configuration on the fly.
 */
public class ChangeLoggingRule implements TestRule {

    private final String configFile;

    public ChangeLoggingRule(String configFile) {
        this.configFile = configFile;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
                context.setConfigLocation(getClass().getClassLoader().getResource(configFile).toURI());
                try {
                    base.evaluate();
                } finally {
                    context.setConfigLocation(null);
                }
            }
        };
    }
}
