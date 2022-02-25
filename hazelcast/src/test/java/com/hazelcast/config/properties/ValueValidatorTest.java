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

package com.hazelcast.config.properties;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ValueValidatorTest {

    @Test(expected = ValidationException.class)
    public void test_ValueValidator_with_cause() {
        ValueValidator<String> valueValidator = new Test1ValueValidator();
        valueValidator.validate("Test");
    }

    @Test(expected = ValidationException.class)
    public void test_ValueValidator_with_message() {
        ValueValidator<Integer> valueValidator = new Test2ValueValidator();
        valueValidator.validate(-1);
    }

    private static class Test1ValueValidator implements ValueValidator<String> {

        @Override
        public void validate(String value)
                throws ValidationException {

            try {
                Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new ValidationException(e);
            }
        }
    }

    private static class Test2ValueValidator implements ValueValidator<Integer> {

        @Override
        public void validate(Integer value)
                throws ValidationException {

            if (value < 0 || value > 65535) {
                throw new ValidationException("value must between 0 and 65535");
            }
        }
    }
}
