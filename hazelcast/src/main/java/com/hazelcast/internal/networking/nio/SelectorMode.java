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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.util.StringUtil;

import static java.lang.String.format;

/**
 * Controls the mode in which IO and acceptor thread selectors will be operating
 */
public enum SelectorMode {
    SELECT,
    SELECT_NOW,
    SELECT_WITH_FIX;

    public static final String SELECT_STRING = "select";
    public static final String SELECT_WITH_FIX_STRING = "selectwithfix";
    public static final String SELECT_NOW_STRING = "selectnow";

    public static SelectorMode getConfiguredValue() {
        return fromString(getConfiguredString());
    }

    public static String getConfiguredString() {
        return System.getProperty("hazelcast.io.selectorMode", SELECT_STRING).trim().toLowerCase(StringUtil.LOCALE_INTERNAL);
    }

    public static SelectorMode fromString(String value) {
        if (value.equals(SELECT_STRING)) {
            return SELECT;
        } else if (value.equals(SELECT_WITH_FIX_STRING)) {
            return SELECT_WITH_FIX;
        } else if (value.equals(SELECT_NOW_STRING) || value.startsWith(SELECT_NOW_STRING + ",")) {
            return SELECT_NOW;
        } else {
            throw new IllegalArgumentException(format("Unrecognized selectorMode [%s]", value));
        }
    }
}
