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

package com.hazelcast.client.console;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.management.MCClusterMetadata;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import org.jline.reader.EOFError;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

import java.io.IOError;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.client.console.HazelcastCommandLine.getClusterMetadata;
import static com.hazelcast.client.console.HazelcastCommandLine.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.trim;

@SuppressWarnings({
        "checkstyle:CyclomaticComplexity",
        "checkstyle:MethodLength",
        "checkstyle:NPathComplexity",
        "checkstyle:TrailingComment"
})
public final class SqlConsole {
    private static final int PRIMARY_COLOR = AttributedStyle.YELLOW;
    private static final int SECONDARY_COLOR = 12;

    private SqlConsole() { }

    public static void run(HazelcastInstance hzClient) {
        LineReader reader = LineReaderBuilder.builder().parser(new MultilineParser())
                .variable(LineReader.SECONDARY_PROMPT_PATTERN, new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR)).append("%M%P > ").toAnsi())
                .variable(LineReader.INDENTATION, 2)
                .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                .appName("hazelcast-sql")
                .build();

        AtomicReference<SqlResult> activeSqlResult = new AtomicReference<>();
        reader.getTerminal().handle(Terminal.Signal.INT, signal -> {
            SqlResult r = activeSqlResult.get();
            if (r != null) {
                r.close();
            }
        });

        PrintWriter writer = reader.getTerminal().writer();
        writer.println(sqlStartingPrompt(hzClient));
        writer.flush();

        for (; ; ) {
            String command;
            try {
                command = reader.readLine(new AttributedStringBuilder()
                        .style(AttributedStyle.DEFAULT.foreground(SECONDARY_COLOR))
                        .append("sql> ").toAnsi()).trim();
            } catch (EndOfFileException | IOError e) {
                // Ctrl+D, and kill signals result in exit
                writer.println(Constants.EXIT_PROMPT);
                writer.flush();
                break;
            } catch (UserInterruptException e) {
                // Ctrl+C cancels the not-yet-submitted query
                continue;
            }

            command = command.trim();
            if (command.length() > 0 && command.charAt(command.length() - 1) == ';') {
                command = command.substring(0, command.length() - 1).trim();
            } else if (command.lastIndexOf(";") >= 0) {
                String errorPrompt = new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                        .append("There are non-whitespace characters after the semicolon")
                        .toAnsi();
                writer.println(errorPrompt);
                writer.flush();
                continue;
            }

            if ("".equals(command)) {
                continue;
            }
            if (equalsIgnoreCase("clear", command)) {
                reader.getTerminal().puts(InfoCmp.Capability.clear_screen);
                continue;
            }
            if (equalsIgnoreCase("help", command)) {
                writer.println(helpPrompt());
                writer.flush();
                continue;
            }
            if (equalsIgnoreCase("history", command)) {
                History hist = reader.getHistory();
                ListIterator<History.Entry> iterator = hist.iterator();
                while (iterator.hasNext()) {
                    History.Entry entry = iterator.next();
                    if (iterator.hasNext()) {
                        String entryLine = new AttributedStringBuilder()
                                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                                .append(String.valueOf(entry.index() + 1))
                                .append(" - ")
                                .append(entry.line())
                                .toAnsi();
                        writer.println(entryLine);
                        writer.flush();
                    } else {
                        // remove the "history" command from the history
                        iterator.remove();
                        hist.resetIndex();
                    }
                }
                continue;
            }
            if (equalsIgnoreCase("exit", command)) {
                writer.println(Constants.EXIT_PROMPT);
                writer.flush();
                break;
            }
            executeSqlCmd(hzClient, command, reader.getTerminal(), activeSqlResult);
        }
    }

    private static void executeSqlCmd(
            HazelcastInstance hz,
            String command,
            Terminal terminal,
            AtomicReference<SqlResult> activeSqlResult
    ) {
        PrintWriter out = terminal.writer();
        try (SqlResult sqlResult = hz.getSql().execute(command)) {
            activeSqlResult.set(sqlResult);

            // if it's a result with an update count, just print it
            if (sqlResult.updateCount() != -1) {
                String message = new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                        .append("OK")
                        .toAnsi();
                out.println(message);
                return;
            }
            SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
            int[] colWidths = determineColumnWidths(rowMetadata);
            Alignment[] alignments = determineAlignments(rowMetadata);

            // this is a result with rows. Print the header and rows, watch for concurrent cancellation
            printMetadataInfo(rowMetadata, colWidths, alignments, out);

            int rowCount = 0;
            for (SqlRow row : sqlResult) {
                rowCount++;
                printRow(row, colWidths, alignments, out);
            }

            // bottom line after all the rows
            printSeparatorLine(sqlResult.getRowMetadata().getColumnCount(), colWidths, out);

            String message = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append(String.valueOf(rowCount))
                    .append(" row(s) selected")
                    .toAnsi();
            out.println(message);
        } catch (HazelcastSqlException e) {
            // the query failed to execute with HazelcastSqlException
            String errorPrompt = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append(e.getMessage())
                    .toAnsi();
            out.println(errorPrompt);
        } catch (Exception e) {
            // the query failed to execute with an unexpected exception
            String unexpectedErrorPrompt = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append("Encountered an unexpected exception while executing the query:\n")
                    .append(e.getMessage())
                    .toAnsi();
            out.println(unexpectedErrorPrompt);
            e.printStackTrace(out);
        }
    }

    private static String sqlStartingPrompt(HazelcastInstance hz) {
        HazelcastClientInstanceImpl hazelcastClientImpl = getHazelcastClientInstanceImpl(hz);
        ClientClusterService clientClusterService = hazelcastClientImpl.getClientClusterService();
        MCClusterMetadata clusterMetadata =
                FutureUtil.getValue(getClusterMetadata(hazelcastClientImpl, clientClusterService.getMasterMember()));
        Cluster cluster = hazelcastClientImpl.getCluster();
        Set<Member> members = cluster.getMembers();
        String versionString = "Hazelcast " + clusterMetadata.getMemberVersion();
        return new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Connected to ")
                .append(versionString)
                .append(" at ")
                .append(members.iterator().next().getAddress().toString())
                .append(" (+")
                .append(String.valueOf(members.size() - 1))
                .append(" more)\n")
                .append("Type 'help' for instructions")
                .toAnsi();
    }

    private static String helpPrompt() {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Available Commands:\n")
                .append("  clear    Clears the terminal screen\n")
                .append("  exit     Exits from the SQL console\n")
                .append("  help     Provides information about available commands\n")
                .append("  history  Shows the command history of the current session\n")
                .append("Hints:\n")
                .append("  Semicolon completes a query\n")
                .append("  Ctrl+C cancels a streaming query\n")
                .append("  https://docs.hazelcast.com/hazelcast/latest/sql/sql-statements.html");
        return builder.toAnsi();
    }


    private enum Alignment {
        LEFT, RIGHT
    }

    private static int[] determineColumnWidths(SqlRowMetadata metadata) {
        int colCount = metadata.getColumnCount();
        int[] colWidths = new int[colCount];
        for (int i = 0; i < colCount; i++) {
            SqlColumnMetadata colMetadata = metadata.getColumn(i);
            SqlColumnType type = colMetadata.getType();
            String colName = colMetadata.getName();
            switch (type) {
                case BOOLEAN:
                    colWidths[i] = determineColumnWidth(colName, Constants.BOOLEAN_FORMAT_LENGTH);
                    break;
                case DATE:
                    colWidths[i] = determineColumnWidth(colName, Constants.DATE_FORMAT_LENGTH);
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    colWidths[i] = determineColumnWidth(colName, Constants.TIMESTAMP_WITH_TIME_ZONE_FORMAT_LENGTH);
                    break;
                case DECIMAL:
                    colWidths[i] = determineColumnWidth(colName, Constants.DECIMAL_FORMAT_LENGTH);
                    break;
                case REAL:
                    colWidths[i] = determineColumnWidth(colName, Constants.REAL_FORMAT_LENGTH);
                    break;
                case DOUBLE:
                    colWidths[i] = determineColumnWidth(colName, Constants.DOUBLE_FORMAT_LENGTH);
                    break;
                case INTEGER:
                    colWidths[i] = determineColumnWidth(colName, Constants.INTEGER_FORMAT_LENGTH);
                    break;
                case NULL:
                    colWidths[i] = determineColumnWidth(colName, Constants.NULL_FORMAT_LENGTH);
                    break;
                case TINYINT:
                    colWidths[i] = determineColumnWidth(colName, Constants.TINYINT_FORMAT_LENGTH);
                    break;
                case SMALLINT:
                    colWidths[i] = determineColumnWidth(colName, Constants.SMALLINT_FORMAT_LENGTH);
                    break;
                case TIMESTAMP:
                    colWidths[i] = determineColumnWidth(colName, Constants.TIMESTAMP_FORMAT_LENGTH);
                    break;
                case BIGINT:
                    colWidths[i] = determineColumnWidth(colName, Constants.BIGINT_FORMAT_LENGTH);
                    break;
                case VARCHAR:
                    colWidths[i] = determineColumnWidth(colName, Constants.VARCHAR_FORMAT_LENGTH);
                    break;
                case OBJECT:
                    colWidths[i] = determineColumnWidth(colName, Constants.OBJECT_FORMAT_LENGTH);
                    break;
                case JSON:
                    colWidths[i] = determineColumnWidth(colName, Constants.JSON_FORMAT_LENGTH);
                    break;
                default:
                    throw new UnsupportedOperationException(type.toString());
            }
        }
        return colWidths;
    }

    private static int determineColumnWidth(String header, int typeLength) {
        return Math.max(Math.min(header.length(), Constants.VARCHAR_FORMAT_LENGTH), typeLength);
    }

    private static Alignment[] determineAlignments(SqlRowMetadata metadata) {
        int colCount = metadata.getColumnCount();
        Alignment[] alignments = new Alignment[colCount];
        for (int i = 0; i < colCount; i++) {
            SqlColumnMetadata colMetadata = metadata.getColumn(i);
            SqlColumnType type = colMetadata.getType();
            switch (type) {
                case BIGINT:
                case DECIMAL:
                case DOUBLE:
                case INTEGER:
                case REAL:
                case SMALLINT:
                case TINYINT:
                    alignments[i] = Alignment.RIGHT;
                    break;
                case BOOLEAN:
                case DATE:
                case NULL:
                case OBJECT:
                case TIMESTAMP:
                case VARCHAR:
                case TIMESTAMP_WITH_TIME_ZONE:
                default:
                    alignments[i] = Alignment.LEFT;
            }
        }
        return alignments;
    }

    private static void printMetadataInfo(SqlRowMetadata metadata, int[] colWidths,
                                          Alignment[] alignments, PrintWriter out) {
        int colCount = metadata.getColumnCount();
        printSeparatorLine(colCount, colWidths, out);
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        for (int i = 0; i < colCount; i++) {
            String colName = metadata.getColumn(i).getName();
            colName = sanitize(colName, colWidths[i]);
            builder.style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR));
            appendAligned(colWidths[i], colName, alignments[i], builder);
            builder.style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
            builder.append('|');
        }
        out.println(builder.toAnsi());
        printSeparatorLine(colCount, colWidths, out);
        out.flush();
    }

    private static void printRow(SqlRow row, int[] colWidths, Alignment[] alignments, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        int columnCount = row.getMetadata().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String colString = row.getObject(i) != null ? sanitize(row.getObject(i).toString(), colWidths[i]) : "NULL";
            builder.style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR));
            appendAligned(colWidths[i], colString, alignments[i], builder);
            builder.style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
            builder.append('|');
        }
        out.println(builder.toAnsi());
        out.flush();
    }

    private static String sanitize(String s, int width) {
        s = s.replace("\n", "\\n");
        if (s.length() > width) {
            s = s.substring(0, width - 1) + "\u2026";
        }
        return s;
    }

    private static void appendAligned(int width, String s, Alignment alignment, AttributedStringBuilder builder) {
        int padding = width - s.length();
        assert padding >= 0;

        if (alignment == Alignment.RIGHT) {
            appendPadding(builder, padding, ' ');
        }
        builder.append(s);
        if (alignment == Alignment.LEFT) {
            appendPadding(builder, padding, ' ');
        }
    }

    private static void appendPadding(AttributedStringBuilder builder, int length, char paddingChar) {
        for (int i = 0; i < length; i++) {
            builder.append(paddingChar);
        }
    }

    private static void printSeparatorLine(int colSize, int[] colWidths, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append('+');
        for (int i = 0; i < colSize; i++) {
            appendPadding(builder, colWidths[i], '-');
            builder.append('+');
        }
        out.println(builder.toAnsi());
    }


    /**
     * A parser for SQL-like inputs. Commands are terminated with a semicolon.
     * It is adapted from
     * <a href="https://github.com/julianhyde/sqlline/blob/master/src/main/java/sqlline/SqlLineParser.java">
     * SqlLineParser</a>
     * which is licensed under the BSD-3-Clause License.
     */
    private static final class MultilineParser extends DefaultParser {

        private MultilineParser() {
        }

        @Override
        public ParsedLine parse(String line, int cursor, Parser.ParseContext context) throws SyntaxError {
            super.setQuoteChars(new char[]{'\'', '"'});
            super.setEofOnUnclosedQuote(true);
            stateCheck(line, cursor);
            return new ArgumentList(line, Collections.emptyList(), -1, -1,
                    cursor, "'", -1, -1);
        }

        private void stateCheck(String line, int cursor) {
            boolean containsNonWhitespaceData = false;
            int quoteStart = -1;
            int oneLineCommentStart = -1;
            int multiLineCommentStart = -1;
            int lastSemicolonIdx = -1;
            for (int i = 0; i < line.length(); i++) {
                // If a one line comment, a multiline comment or a quote is not started before,
                // check if the character we're on is a quote character
                if (oneLineCommentStart == -1
                        && multiLineCommentStart == -1
                        && quoteStart < 0
                        && (isQuoteChar(line, i))) {
                    // Start a quote block
                    quoteStart = i;
                    containsNonWhitespaceData = true;
                } else {
                    char currentChar = line.charAt(i);
                    if (quoteStart >= 0) {
                        // In a quote block
                        if ((line.charAt(quoteStart) == currentChar) && !isEscaped(line, i)) {
                            // End the block; arg could be empty, but that's fine
                            quoteStart = -1;
                        }
                    } else if (oneLineCommentStart == -1
                            && line.regionMatches(i, "/*", 0, "/*".length())) {
                        // Enter the multiline comment block
                        multiLineCommentStart = i;
                        containsNonWhitespaceData = true;
                    } else if (multiLineCommentStart >= 0) {
                        // In a multiline comment block
                        if (i - multiLineCommentStart > 2
                                && line.regionMatches(i - 1, "*/", 0, "*/".length())) {
                            // End the multiline block
                            multiLineCommentStart = -1;
                        }
                    } else if (oneLineCommentStart == -1
                            && line.regionMatches(i, "--", 0, "--".length())) {
                        // Enter the one line comment block
                        oneLineCommentStart = i;
                        containsNonWhitespaceData = true;
                    } else if (oneLineCommentStart >= 0) {
                        // In a one line comment
                        if (currentChar == '\n') {
                            // End the one line comment block
                            oneLineCommentStart = -1;
                        }
                    } else {
                        // Not in a quote or comment block
                        if (currentChar == ';') {
                            lastSemicolonIdx = i;
                        } else if (!Character.isWhitespace(currentChar)) {
                            containsNonWhitespaceData = true;
                        }
                    }
                }
            }

            if (Constants.COMMAND_SET.contains(lowerCaseInternal(trim(line)))) {
                return;
            }
            // These EOFError exceptions are captured in LineReader's
            // readLine() method and it points out that the command
            // being written to console is not finalized and command
            // won't be read
            if (isEofOnEscapedNewLine() && isEscapeChar(line, line.length() - 1)) {
                throw new EOFError(-1, cursor, "Escaped new line");
            }

            if (isEofOnUnclosedQuote() && quoteStart >= 0) {
                throw new EOFError(-1, quoteStart, "Missing closing quote",
                        line.charAt(quoteStart) == '\'' ? "quote" : "dquote");
            }

            if (oneLineCommentStart != -1) {
                throw new EOFError(-1, cursor, "One line comment");
            }

            if (multiLineCommentStart != -1) {
                throw new EOFError(-1, cursor, "Missing end of comment", "**");
            }

            if (containsNonWhitespaceData
                    && (lastSemicolonIdx == -1 || lastSemicolonIdx >= cursor)) {
                throw new EOFError(-1, cursor, "Missing semicolon (;)");
            }
        }
    }

    private static class Constants {

        static final Set<String> COMMAND_SET = new HashSet<>(Arrays.asList("clear", "exit", "help", "history"));
        static final String EXIT_PROMPT = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Exiting from SQL console")
                .toAnsi();
        static final Integer BOOLEAN_FORMAT_LENGTH = 5;
        static final Integer BIGINT_FORMAT_LENGTH = 20;
        static final Integer DATE_FORMAT_LENGTH = 10;
        static final Integer DECIMAL_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer DOUBLE_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer INTEGER_FORMAT_LENGTH = 12;
        static final Integer NULL_FORMAT_LENGTH = 4;
        static final Integer REAL_FORMAT_LENGTH = 25; // it has normally unlimited precision
        static final Integer OBJECT_FORMAT_LENGTH = 20; // it has normally unlimited precision
        static final Integer TINYINT_FORMAT_LENGTH = 4;
        static final Integer SMALLINT_FORMAT_LENGTH = 6;
        static final Integer TIMESTAMP_FORMAT_LENGTH = 19;
        static final Integer TIMESTAMP_WITH_TIME_ZONE_FORMAT_LENGTH = 25;
        static final Integer VARCHAR_FORMAT_LENGTH = 20; // it has normally unlimited precision
        static final Integer JSON_FORMAT_LENGTH = 40;
    }

}
