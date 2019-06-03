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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Like Predicate
 */
@BinaryInterface
public class LikePredicate extends AbstractPredicate {

    private static final long serialVersionUID = 1L;

    protected String expression;
    private transient volatile Pattern pattern;

    public LikePredicate() {
    }

    public LikePredicate(String attributeName, String expression) {
        super(attributeName);
        this.expression = expression;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        String attributeValueString = (String) attributeValue;
        if (attributeValueString == null) {
            return (expression == null);
        }

        if (expression == null) {
            return false;
        }

        pattern = pattern != null ? pattern : createPattern(expression);
        Matcher m = pattern.matcher(attributeValueString);
        return m.matches();
    }

    private Pattern createPattern(String expression) {
        // we quote the input string then escape then replace % and _
        // at the end we have a regex pattern look like: \QSOME_STRING\E.*\QSOME_OTHER_STRING\E
        final String quotedExpression = Pattern.quote(expression);
        String regex = quotedExpression
                //escaped %
                .replaceAll("(?<!\\\\)[%]", "\\\\E.*\\\\Q")
                //escaped _
                .replaceAll("(?<!\\\\)[_]", "\\\\E.\\\\Q")
                //non escaped %
                .replaceAll("\\\\%", "%")
                //non escaped _
                .replaceAll("\\\\_", "_");
        int flags = getFlags();
        return Pattern.compile(regex, flags);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(expression);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        expression = in.readUTF();
    }

    protected int getFlags() {
        return Pattern.DOTALL;
    }

    @Override
    public String toString() {
        return attributeName + " LIKE '" + expression + "'";
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.LIKE_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof LikePredicate)) {
            return false;
        }

        LikePredicate that = (LikePredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        return expression != null ? expression.equals(that.expression) : that.expression == null;
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof LikePredicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (expression != null ? expression.hashCode() : 0);
        return result;
    }
}
