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

package com.hazelcast.query.impl;

import com.hazelcast.query.extractor.ArgumentParser;

/**
 * ArgumentParser that's used by default. It simply passes the given object through - without doing any parsing.
 *
 * @see ArgumentParser
 */
public class DefaultArgumentParser extends ArgumentParser<Object, Object> {

    @Override
    public Object parse(Object input) {
        return input;
    }

}
