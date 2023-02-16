/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.function;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.schema.DdlUnparseable;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A class stored in the SQL catalog to represent a function created using the
 * CREATE FUNCTION command.
 */
public class UserDefinedFunction implements IdentifiedDataSerializable, Serializable, DdlUnparseable {
    private String name;
    private String language;
    private QueryDataType returnType;
    private List<String> parameterNames;
    private List<QueryDataType> parameterTypes;
    private String body;

    public UserDefinedFunction() {
    }

    public UserDefinedFunction(String name, String language,
                               QueryDataType returnType,
                               List<String> parameterNames, List<QueryDataType> parameterTypes,
                               String body) {
        this.name = name;
        this.language = language;
        this.returnType = returnType;
        this.parameterNames = parameterNames;
        this.parameterTypes = parameterTypes;
        this.body = body;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    @Nonnull
    public String unparse() {
        StringBuffer buffer = new StringBuffer();

        buffer.append("CREATE FUNCTION");
        buffer.append(" \"").append(name).append("\" ");

        buffer.append("(");
        if (parameterNames.size() > 0) {
            for (int i = 0; i < parameterNames.size(); ++i) {
                buffer.append(parameterNames.get(i)).append(" ");
                buffer.append(parameterTypes.get(i).getTypeFamily().toString());
                if (i < parameterNames.size() - 1) {
                    buffer.append(", ");
                }
            }
        }
        buffer.append(") ");

        buffer.append("RETURNS ");
        buffer.append(returnType.getTypeFamily().toString());
        buffer.append("\n");
        buffer.append("LANGUAGE '");
        buffer.append(language);
        buffer.append("'\nAS `");
        // TODO: escape backticks
        buffer.append(body);
        buffer.append("`");

        return buffer.toString();
    }

    private static void appendOption(StringBuffer buffer, String optionKey, String optionValue, boolean first) {
        if (first) {
            buffer.append("'");
        } else {
            buffer.append(", '");
        }

        buffer.append(optionKey)
                .append("'")
                .append(" = ")
                .append("'")
                .append(optionValue)
                .append("'");
    }

    private static void appendOption(StringBuffer buffer, String optionKey, Integer optionValue, boolean first) {
        if (first) {
            buffer.append("'");
        } else {
            buffer.append(", '");
        }

        buffer.append(optionKey)
                .append("'")
                .append(" = ")
                .append("'")
                .append(optionValue)
                .append("'");
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(language);
        out.writeObject(returnType);

        out.writeInt(parameterNames.size());
        for (final String param : parameterNames) {
            out.writeString(param);
        }
        for (final QueryDataType param : parameterTypes) {
            out.writeObject(param);
        }
        out.writeString(body);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        this.name = in.readString();
        this.language = in.readString();
        this.returnType = in.readObject();

        int paramCount = in.readInt();
        this.parameterNames = new ArrayList<>(paramCount);
        this.parameterTypes = new ArrayList<>(paramCount);

        for (int i = 0; i < paramCount; i++) {
            this.parameterNames.add(in.readString());
        }
        for (int i = 0; i < paramCount; i++) {
            this.parameterTypes.add(in.readObject());
        }
        this.body = in.readString();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.UDF;
    }

    public String getLanguage() {
        return language;
    }

    public QueryDataType getReturnType() {
        return returnType;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public List<QueryDataType> getParameterTypes() {
        return parameterTypes;
    }

    public String getBody() {
        return body;
    }
}
