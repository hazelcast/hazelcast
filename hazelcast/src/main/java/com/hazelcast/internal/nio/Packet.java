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

package com.hazelcast.internal.nio;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.serialization.impl.HeapData;

import static com.hazelcast.internal.nio.PacketIOHelper.HEADER_SIZE;

/**
 * A Packet is a piece of data sent over the wire. The Packet is used for member to member communication.
 * <p>
 * The Packet extends HeapData instead of wrapping it. From a design point of view this is often
 * not the preferred solution (prefer composition over inheritance), but in this case that
 * would mean more object litter.
 * <p>
 * Since the Packet isn't used throughout the system, this design choice is visible locally.
 */
// Declaration order suppressed due to private static int FLAG_TYPEx declarations
@SuppressWarnings({"checkstyle:declarationorder", "checkstyle:magicnumber"})
public final class Packet extends HeapData implements OutboundFrame {

    public static final byte VERSION = 4;


    //             PACKET HEADER FLAGS
    //
    // Flags are dispatched against in a cascade:
    // 1. URGENT (bit 4)
    // 2. Packet type (bits 0, 2, 5)
    // 3. Flags specific to a given packet type (bits 1, 6)
    // 4. 4.x flag (bit 7)

    // 1. URGENT flag

    /**
     * Marks the packet as Urgent
     */
    public static final int FLAG_URGENT = 1 << 4;


    // 2. Packet type flags, encode up to 7 packet types.
    //
    // When adding a new packet type, DO NOT ADD MORE TYPE FLAGS. Instead rename one of the
    // Packet.Type.UNDEFINEDx members to represent the new type.
    //
    // Historically the first three packet types were encoded as separate, mutually exclusive flags.
    // These are given below. The enum Packet.Type should be used to encode/decode the type from the
    // header flags bitfield.

    /**
     * Packet type bit 0. Historically the OPERATION type flag.
     */
    private static final int FLAG_TYPE0 = 1 << 0;
    /**
     * Packet type bit 1. Historically the EVENT type flag.
     */
    private static final int FLAG_TYPE1 = 1 << 2;
    /**
     * Packet type bit 2. Historically the BIND type flag.
     */
    private static final int FLAG_TYPE2 = 1 << 5;

    // 3. Type-specific flags. Same bits can be reused within each type

    // 3.a Operation packet flags

    /**
     * Marks an Operation packet as Response
     */
    public static final int FLAG_OP_RESPONSE = 1 << 1;
    /**
     * Marks an Operation packet as Operation control (like invocation-heartbeats)
     */
    public static final int FLAG_OP_CONTROL = 1 << 6;

    // 3.b Jet packet flags

    /**
     * Marks a Jet packet as Flow control
     */
    public static final int FLAG_JET_FLOW_CONTROL = 1 << 1;

    /**
     * Marks a packet as sent by a 4.x member
     */
    public static final int FLAG_4_0 = 1 << 7;

    //            END OF HEADER FLAG SECTION


    // char is a 16-bit unsigned integer. Here we use it as a bitfield.
    private char flags;

    private int partitionId;
    private transient ServerConnection conn;

    public Packet() {
        raiseFlags(FLAG_4_0);
    }

    public Packet(byte[] payload) {
        this(payload, -1);
    }

    public Packet(byte[] payload, int partitionId) {
        super(payload);
        this.partitionId = partitionId;
        raiseFlags(FLAG_4_0);
    }

    /**
     * Gets the Connection this Packet was send with.
     *
     * @return the Connection. Could be null.
     */
    public ServerConnection getConn() {
        return conn;
    }

    /**
     * Sets the Connection this Packet is send with.
     * <p>
     * This is done on the reading side of the Packet to make it possible to retrieve information about
     * the sender of the Packet.
     *
     * @param conn the connection.
     * @return {@code this} (for fluent interface)
     */
    public Packet setConn(ServerConnection conn) {
        this.conn = conn;
        return this;
    }

    public Type getPacketType() {
        return Type.fromFlags(flags);
    }

    /**
     * Sets the packet type by updating the packet type bits in the {@code flags} bitfield. Other bits
     * are unaffected.
     *
     * @param type the type to set
     * @return {@code this} (for fluent interface)
     */
    public Packet setPacketType(Packet.Type type) {
        int nonTypeFlags = flags & (~FLAG_TYPE0 & ~FLAG_TYPE1 & ~FLAG_TYPE2);
        resetFlagsTo(type.headerEncoding | nonTypeFlags);
        return this;
    }

    /**
     * Raises all the flags raised in the argument. Does not lower any flags.
     *
     * @param flagsToRaise the flags to raise
     * @return {@code this} (for fluent interface)
     */
    public Packet raiseFlags(int flagsToRaise) {
        flags |= flagsToRaise;
        return this;
    }

    /**
     * Resets the entire {@code flags} bitfield to the supplied value. This also affects the packet type bits.
     *
     * @param flagsToSet the flags. Only the least significant two bytes of the argument are used.
     * @return {@code this} (for fluent interface)
     */
    public Packet resetFlagsTo(int flagsToSet) {
        flags = (char) flagsToSet;
        return this;
    }

    /**
     * Returns {@code true} if any of the flags supplied in the argument are set.
     *
     * @param flagsToCheck the flags to check
     * @return {@code true} if any of the flags is set, {@code false} otherwise.
     */
    public boolean isFlagRaised(int flagsToCheck) {
        return isFlagRaised(flags, flagsToCheck);
    }

    private static boolean isFlagRaised(char flags, int flagsToCheck) {
        return (flags & flagsToCheck) != 0;
    }

    /**
     * @return the complete flags bitfield as a {@code char}.
     */
    public char getFlags() {
        return flags;
    }

    /**
     * Returns the partition ID of this packet. If this packet is not for a particular partition, -1 is returned.
     *
     * @return the partition ID.
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean isUrgent() {
        return isFlagRaised(FLAG_URGENT);
    }

    @Override
    public int getFrameLength() {
        return (payload != null ? totalSize() : 0) + HEADER_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Packet)) {
            return false;
        }

        Packet packet = (Packet) o;
        if (!super.equals(packet)) {
            return false;
        }

        if (flags != packet.flags) {
            return false;
        }
        return partitionId == packet.partitionId;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) flags;
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        Type type = getPacketType();
        return "Packet{"
                + "partitionId=" + partitionId
                + ", frameLength=" + getFrameLength()
                + ", conn=" + conn
                + ", rawFlags=" + Integer.toBinaryString(flags)
                + ", isUrgent=" + isUrgent()
                + ", packetType=" + type.name()
                + ", typeSpecificFlags=" + type.describeFlags(flags)
                + '}';
    }


    public enum Type {
        /**
         * Represents "missing packet type", consists of all zeros. A zeroed-out packet header would
         * resolve to this type.
         * <p>
         * {@code ordinal = 0}
         */
        NULL,
        /**
         * The type of an Operation packet.
         * <p>
         * {@code ordinal = 1}
         */
        OPERATION {
            @Override
            public String describeFlags(char flags) {
                return "[isResponse=" + isFlagRaised(flags, FLAG_OP_RESPONSE)
                        + ", isOpControl=" + isFlagRaised(flags, FLAG_OP_CONTROL) + ']';
            }
        },
        /**
         * The type of an Event packet.
         * <p>
         * {@code ordinal = 2}
         */
        EVENT,
        /**
         * The type of a Jet packet.
         * <p>
         * {@code ordinal = 3}
         */
        JET {
            @Override
            public String describeFlags(char flags) {
                return "[isFlowControl=" + isFlagRaised(flags, FLAG_JET_FLOW_CONTROL) + ']';
            }
        },
        /**
         * TcpServer specific control messages.
         * <p>
         * {@code ordinal = 4}
         */
        SERVER_CONTROL,
        /**
         * Unused packet type. Available for future use.
         * <p>
         * {@code ordinal = 5}
         */
        UNDEFINED5,
        /**
         * Unused packet type. Available for future use.
         * <p>
         * {@code ordinal = 6}
         */
        UNDEFINED6,
        /**
         * Unused packet type. Available for future use.
         * <p>
         * {@code ordinal = 7}
         */
        UNDEFINED7;

        final char headerEncoding;

        private static final Type[] VALUES = values();

        Type() {
            headerEncoding = (char) encodeOrdinal();
        }

        public static Type fromFlags(int flags) {
            return VALUES[headerDecode(flags)];
        }

        public String describeFlags(char flags) {
            return "<NONE>";
        }

        @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
        private int encodeOrdinal() {
            final int ordinal = ordinal();
            assert ordinal < 8 : "Ordinal out of range for member " + name() + ": " + ordinal;
            return (ordinal & 0x01)
                    | (ordinal & 0x02) << 1
                    | (ordinal & 0x04) << 3;
        }

        @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
        private static int headerDecode(int flags) {
            return (flags & FLAG_TYPE0)
                    | (flags & FLAG_TYPE1) >> 1
                    | (flags & FLAG_TYPE2) >> 3;
        }
    }
}
