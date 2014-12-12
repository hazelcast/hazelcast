/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicate;

import java.util.regex.Pattern;

/**
 * Ilike Predicate
 */
public class ILikePredicate extends LikePredicate {

    public ILikePredicate() {
    }

    public ILikePredicate(String attribute, String second) {
        super(attribute, second);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(attribute)
                .append(" ILIKE '")
                .append(second)
                .append("'");
        return builder.toString();
    }


    @Override
    protected int getFlags() {
        return Pattern.CASE_INSENSITIVE;
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof LikePredicate) {
            LikePredicate p = (LikePredicate) predicate;
            if (p.attribute == null || attribute == null) {
                return p.attribute == attribute;
            }

            return p.attribute.toLowerCase().equals(attribute.toLowerCase()) && p.second.equals(second);
        }
        return false;
    }

    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (attribute != null ? attribute.hashCode() : 0);
        result = 31 * result + (second != null ? second.toLowerCase().hashCode() : 0);
        return result;
    }
}
