/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import java.util.ArrayList;

public class ExternalNameIdentifiers {

    private String catalog;

    private String schemaName;

    private String tableName;

    public String getCatalog() {
        return catalog;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void parseExternalTableName(String externalTableName) {
        ArrayList<String> identifiers = getIdentifiers(externalTableName);
        int size = identifiers.size();

        // Assign according to number of identifiers
        if (size == 3) {
            catalog = identifiers.get(0);
            schemaName = identifiers.get(1);
            tableName = identifiers.get(2);
        } else if (size == 2) {
            schemaName = identifiers.get(0);
            tableName = identifiers.get(1);
        } else if (size == 1) {
            tableName = identifiers.get(0);
        } else {
            throw new IllegalArgumentException("No identifiers found");
        }
    }

    protected ArrayList<String> getIdentifiers(String externalTableName) {
        ArrayList<String> identifiers = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder(externalTableName);

        boolean expectClosingQuote;

        while ((stringBuilder.length() > 0)) {

            char firstChar = stringBuilder.charAt(0);
            String closingQuote;

            if (firstChar == '`') {
                // Quote is used. Separator is the quote itself. Closing quote must exist
                closingQuote = "`";
                expectClosingQuote = true;
            } else if (firstChar == '\"') {
                // Quote is used. Separator is the quote itself. . Closing quote must exist
                closingQuote = "\"";
                expectClosingQuote = true;
            } else {
                // No quote is used. Separator the dot character. Closing quote may not exist
                closingQuote = ".";
                expectClosingQuote = false;
            }

            int closingQuoteIndex = findClosingQuoteIndex(stringBuilder, expectClosingQuote, closingQuote);

            String identifier = substringIdentifier(stringBuilder, expectClosingQuote, closingQuoteIndex);
            identifiers.add(identifier);

            checkDotSeparatorExists(stringBuilder);
        }
        return identifiers;
    }

    private int findClosingQuoteIndex(StringBuilder stringBuilder, boolean expectClosingQuote, String closingQuote) {
        int closingQuoteIndex = stringBuilder.indexOf(closingQuote, 1);
        if (expectClosingQuote && closingQuoteIndex == -1) {
            throw new IllegalArgumentException("Closing quote not found");
        }
        return closingQuoteIndex;
    }

    private String substringIdentifier(StringBuilder stringBuilder, boolean expectClosingQuote, int closingQuoteIndex) {
        String identifier;

        // If closing quote found
        if (closingQuoteIndex != -1) {

            int endIndex;

            if (expectClosingQuote) {

                // Do not include the starting quote
                identifier = stringBuilder.substring(1, closingQuoteIndex);

                // The closing quote must be deleted
                endIndex = closingQuoteIndex + 1;
            } else {

                identifier = stringBuilder.substring(0, closingQuoteIndex);

                endIndex = closingQuoteIndex;
            }

            //Delete the identifier from stringBuilder
            stringBuilder.delete(0, endIndex);
        } else {
            // Closing quote found. Everything is the identifier
            identifier = stringBuilder.toString();
            // Clear the stringBuilder
            stringBuilder.setLength(0);
        }
        return identifier;
    }

    private void checkDotSeparatorExists(StringBuilder stringBuilder) {
        // Check if separator exists
        if (stringBuilder.length() > 0) {
            if (stringBuilder.charAt(0) != '.') {
                throw new IllegalArgumentException("Dot separator not found");
            }
            stringBuilder.deleteCharAt(0);
        }
    }
}