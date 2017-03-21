/*
 * Copyright (c) 2016, Microsoft Corporation. All Rights Reserved.
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

package com.hazelcast.aws;

import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AwsPropertiesTest extends HazelcastTestSupport {
    @Test(expected = ValidationException.class)
    public void testPortValueValidator_validate_negative_val() throws Exception {
        final AwsProperties.PortValueValidator validator = new AwsProperties.PortValueValidator();
        validator.validate(-1);
    }

    @Test(expected = ValidationException.class)
    public void testPortValueValidatorValidateTooBig() throws Exception {
        final AwsProperties.PortValueValidator validator = new AwsProperties.PortValueValidator();
        validator.validate(65536);
    }

    @Test
    public void testPortValueValidatorValidate() throws Exception {
        final AwsProperties.PortValueValidator validator = new AwsProperties.PortValueValidator();
        validator.validate(0);
        validator.validate(1000);
        validator.validate(65535);
    }
}
