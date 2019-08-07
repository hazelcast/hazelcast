package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.DataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

public class TemporalUtils {
    /**
     * Convert provided string to any supported date/time object.
     *
     * @param input Input.
     * @return Mathing date/time.
     */
    public static TemporalValue parseAny(String input) {
        if (input == null)
            return null;

        try {
            if (input.contains("T") || input.contains("t")) {
                // Looks like it is a timestamp.
                if (input.contains("+")) {
                    // Time zone present.
                    return new TemporalValue(
                        DataType.TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME,
                        OffsetDateTime.parse(input)
                    );
                }
                else {
                    // No time zone.
                    return new TemporalValue(DataType.TIMESTAMP, LocalDateTime.parse(input));
                }
            }
            else if (input.contains("-")) {
                // Looks like it is a date.
                return new TemporalValue(DataType.DATE, LocalDate.parse(input));
            }
            else {
                // Otherwise it is a time.
                return new TemporalValue(DataType.TIME, LocalTime.parse(input));
            }
        }
        catch (DateTimeParseException ignore) {
            throw new HazelcastSqlException(-1, "Failed to parse a string to DATE/TIME: " + input);
        }
    }

    private TemporalUtils() {
        // No-op.
    }
}
