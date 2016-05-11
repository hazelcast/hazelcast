/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.util.StringUtil;

/**
 * Controls the mode in which IO and acceptor thread selectors will be operating
 */
public enum SelectorMode {
    SELECT,
    SELECT_NOW,
    SELECT_WITH_FIX;

    public static SelectorMode getConfiguredValue() {
        return fromString(System.getProperty("hazelcast.io.selectorMode"));
    }

    public static SelectorMode fromString(String value) {
        String valueToCheck = StringUtil.isNullOrEmptyAfterTrim(value) ? null : value.trim().toLowerCase();
        if (valueToCheck == null) {
            return SELECT;
        } else if (valueToCheck.equals("selectnow")) {
            return SELECT_NOW;
        } else if (valueToCheck.equals("selectwithfix")) {
            return SELECT_WITH_FIX;
        } else {
            return SELECT;
        }
    }
}
