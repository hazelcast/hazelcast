/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.kafka.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * A class that has a field of every supported canonical type in SQL.
 */
@SuppressWarnings("unused") // getters-setters are used through reflection
public final class AllCanonicalTypesValue implements Serializable {

    private String string;
    private boolean boolean0;
    private byte byte0;
    private short short0;
    private int int0;
    private long long0;
    private float float0;
    private double double0;
    private BigDecimal decimal;
    private LocalTime time;
    private LocalDate date;
    private LocalDateTime timestamp;
    private OffsetDateTime timestampTz;
    private Object object;

    public AllCanonicalTypesValue() {
    }

    @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:ExecutableStatementCount"})
    public AllCanonicalTypesValue(String string, boolean boolean0, byte byte0, short short0, int int0, long long0,
                                  float float0, double double0, BigDecimal decimal, LocalTime time,
                                  LocalDate date, LocalDateTime timestamp, OffsetDateTime timestampTz, Object object
    ) {
        this.string = string;
        this.boolean0 = boolean0;
        this.byte0 = byte0;
        this.short0 = short0;
        this.int0 = int0;
        this.long0 = long0;
        this.float0 = float0;
        this.double0 = double0;
        this.decimal = decimal;
        this.time = time;
        this.date = date;
        this.timestamp = timestamp;
        this.timestampTz = timestampTz;
        this.object = object;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public boolean isBoolean0() {
        return boolean0;
    }

    public void setBoolean0(boolean boolean0) {
        this.boolean0 = boolean0;
    }

    public byte getByte0() {
        return byte0;
    }

    public void setByte0(byte byte0) {
        this.byte0 = byte0;
    }

    public short getShort0() {
        return short0;
    }

    public void setShort0(short short0) {
        this.short0 = short0;
    }

    public int getInt0() {
        return int0;
    }

    public void setInt0(int int0) {
        this.int0 = int0;
    }

    public long getLong0() {
        return long0;
    }

    public void setLong0(long long0) {
        this.long0 = long0;
    }

    public float getFloat0() {
        return float0;
    }

    public void setFloat0(float float0) {
        this.float0 = float0;
    }

    public double getDouble0() {
        return double0;
    }

    public void setDouble0(double double0) {
        this.double0 = double0;
    }

    public BigDecimal getDecimal() {
        return decimal;
    }

    public void setDecimal(BigDecimal decimal) {
        this.decimal = decimal;
    }

    public LocalTime getTime() {
        return time;
    }

    public void setTime(LocalTime time) {
        this.time = time;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public OffsetDateTime getTimestampTz() {
        return timestampTz;
    }

    public void setTimestampTz(OffsetDateTime timestampTz) {
        this.timestampTz = timestampTz;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public String toString() {
        return "AllTypesValue{" +
                "string='" + string + '\'' +
                ", boolean0=" + boolean0 +
                ", byte0=" + byte0 +
                ", short0=" + short0 +
                ", int0=" + int0 +
                ", long0=" + long0 +
                ", float0=" + float0 +
                ", double0=" + double0 +
                ", decimal=" + decimal +
                ", time=" + time +
                ", date=" + date +
                ", timestamp=" + timestamp +
                ", timestampTz=" + timestampTz +
                ", object=" + object +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllCanonicalTypesValue that = (AllCanonicalTypesValue) o;
        return boolean0 == that.boolean0 &&
                byte0 == that.byte0 &&
                short0 == that.short0 &&
                int0 == that.int0 &&
                long0 == that.long0 &&
                Float.compare(that.float0, float0) == 0 &&
                Double.compare(that.double0, double0) == 0 &&
                Objects.equals(string, that.string) &&
                Objects.equals(decimal, that.decimal) &&
                Objects.equals(time, that.time) &&
                Objects.equals(date, that.date) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(timestampTz, that.timestampTz) &&
                Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string,
                boolean0,
                byte0,
                short0,
                int0,
                long0,
                float0,
                double0,
                decimal,
                time,
                date,
                timestamp,
                timestampTz,
                object
        );
    }
}
