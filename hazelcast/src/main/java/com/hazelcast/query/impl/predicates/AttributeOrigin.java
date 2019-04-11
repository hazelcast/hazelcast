/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.QueryConstants;

public enum AttributeOrigin {

    VALUE,
    KEY,
    WITHIN_KEY,
    WITHIN_VALUE;

    public static AttributeOrigin fromName(String attributeName) {
        if (attributeName.equals(QueryConstants.KEY_ATTRIBUTE_NAME.value())) {
            return KEY;
        } else if (attributeName.equals(QueryConstants.THIS_ATTRIBUTE_NAME.value())) {
            return VALUE;
        } else if (attributeName.startsWith(QueryConstants.KEY_ATTRIBUTE_NAME.value())) {
            return WITHIN_KEY;
        } else {
            return WITHIN_VALUE;
        }
    }
}
