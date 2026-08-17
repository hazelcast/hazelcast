/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

import static java.time.temporal.ChronoUnit.MINUTES;

public class TransactionStronglyTyped implements Comparable<TransactionStronglyTyped> {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final Comparator<TransactionStronglyTyped> COMPARATOR = Comparator
            .comparing((TransactionStronglyTyped t) -> t.tokenId)
            .thenComparing(TransactionStronglyTyped::getTxnDatetimeFormatted)
            .thenComparing(TransactionStronglyTyped::getDatetimeWithPrecisionFormatted)
            .thenComparing(TransactionStronglyTyped::getProcessingDateFormatted)
            .thenComparing(t -> t.processingLocalDate)
            .thenComparing(TransactionStronglyTyped::getTimeFormatted)
            .thenComparing(TransactionStronglyTyped::getCreateDatetimeFormatted);

    @JsonProperty("ID")
    public int tokenId;

    @JsonProperty("TXN_DATETIME")
    public Date txnDatetime;

    @JsonProperty("DATETIME_WITH_PRECISION")
    public Instant datetimeWithPrecision;

    @JsonProperty("DATE")
    public Date processingDate;

    @JsonProperty("DATE_LOCAL")
    public LocalDate processingLocalDate;

    @JsonProperty("TIME")
    public Duration time;

    @JsonProperty("CREATE_DATETIME")
    public Instant createDatetime;

    // ser-de
    @SuppressWarnings("unused")
    TransactionStronglyTyped() {
    }

    TransactionStronglyTyped(int tokenId, String txnDatetime, String datetimeWithPrecision,
                             String processingDate, String time, String createDatetime) {
        this.tokenId = tokenId;
        var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            this.txnDatetime = sdf.parse(txnDatetime);
            this.datetimeWithPrecision = Instant.parse(datetimeWithPrecision);
            this.processingDate = DATE_FORMAT.parse(processingDate);
            this.processingLocalDate = LocalDate.parse(processingDate);
            this.time = Duration.parse(time);
            this.createDatetime = Instant.parse(createDatetime);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    static TransactionStronglyTyped from(int tokenId, long txnDatetime,
                                         long datetimeWithPrecision,
                                         long processingDate,
                                         String processingDateLocal,
                                         long time, long createDatetime) {
        var t = new TransactionStronglyTyped();
        t.tokenId = tokenId;
        t.txnDatetime = new Date(txnDatetime);
        t.datetimeWithPrecision = Instant.ofEpochMilli(datetimeWithPrecision);
        t.processingDate = new Date(processingDate);
        t.processingLocalDate = LocalDate.parse(processingDateLocal);
        t.time = Duration.ofMillis(time);
        t.createDatetime = Instant.EPOCH.plusNanos(createDatetime);
        return t;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionStronglyTyped that)) {
            return false;
        }
        return this.compareTo(that) == 0;
    }
    private Instant getCreateDatetimeFormatted() {
        return createDatetime.truncatedTo(MINUTES);
    }

    private long getTimeFormatted() {
        return time.getSeconds();
    }

    private String getProcessingDateFormatted() {
        return DATE_FORMAT.format(processingDate);
    }

    private long getDatetimeWithPrecisionFormatted() {
        return datetimeWithPrecision.getEpochSecond();
    }

    private String getTxnDatetimeFormatted() {
        return DATE_TIME_FORMAT.format(txnDatetime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenId, getTxnDatetimeFormatted(),
                            getDatetimeWithPrecisionFormatted(), getProcessingDateFormatted(), processingLocalDate,
                            getTimeFormatted(), getCreateDatetimeFormatted());
    }

    @Override
    public String toString() {
        return "TransactionStronglyTyped{"
                + "tokenId=" + tokenId
                + ", txnDatetime=" + getTxnDatetimeFormatted()
                + ", datetimeWithPrecision=" + getDatetimeWithPrecisionFormatted()
                + ", processingDate=" + getProcessingDateFormatted()
                + ", processingLocalDate=" + processingLocalDate
                + ", time=" + getTimeFormatted()
                + ", createDatetime=" + getCreateDatetimeFormatted()
                + '}';
    }

    @Override
    public int compareTo(@Nonnull TransactionStronglyTyped o) {
        return COMPARATOR.compare(this, o);
    }
}
