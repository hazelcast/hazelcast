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

package com.hazelcast.internal.adapter;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;

public class ICacheReplaceEntryProcessor implements EntryProcessor<Integer, String, String>, Serializable {

    private static final long serialVersionUID = -396575576353368113L;

    @Override
    public String process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
        String value = entry.getValue();
        if (value == null) {
            return null;
        }

        String oldString = (String) arguments[0];
        String newString = (String) arguments[1];
        String result = value.replace(oldString, newString);
        entry.setValue(result);
        return result;
    }
}
