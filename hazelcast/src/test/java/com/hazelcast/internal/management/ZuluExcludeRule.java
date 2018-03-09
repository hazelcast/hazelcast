/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertNotNull;

/**
 * Exclusion rule for Zulu JDK 6 and 7.
 */
public class ZuluExcludeRule implements MethodRule {

    private static final boolean EXCLUDED;

    static {
        String propertyName = "java.specification.version";
        String versionProperty = System.getProperty(propertyName);
        assertNotNull(propertyName + " should be set!", versionProperty);

        String vendorPropertyName = "java.vm.vendor";
        String vendor = System.getProperty(vendorPropertyName);
        assertNotNull(vendorPropertyName + " should be set!", vendor);

        int version = parseInt(versionProperty.contains(".") ? versionProperty.split("\\.")[1] : versionProperty);
        EXCLUDED = version < 8 && vendor.startsWith("Azul");
    }

    @Override
    public Statement apply(Statement statement, FrameworkMethod frameworkMethod, Object o) {
        if (EXCLUDED) {
            return new ExcludedStatement(frameworkMethod);
        } else {
            return statement;
        }
    }

    private static final class ExcludedStatement extends Statement {

        private final FrameworkMethod method;

        private ExcludedStatement(FrameworkMethod method) {
            this.method = method;
        }

        @Override
        public void evaluate() throws Throwable {
            String className = method.getMethod().getDeclaringClass().getName();
            String methodName = method.getName();

            System.out.println("EXCLUDING '" + className + "#" + methodName + "()'...");
        }
    }
}
