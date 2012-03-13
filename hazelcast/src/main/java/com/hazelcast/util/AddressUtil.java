/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.Collection;
import java.util.LinkedList;

public final class AddressUtil {

    private AddressUtil() {
    }

    public static boolean matchAnyInterface(final String address, final Collection<String> interfaces) {
        if (interfaces == null || interfaces.size() == 0) return false;

        for (final String interfaceMask : interfaces) {
            if (matchInterface(address, interfaceMask)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matchInterface(final String address, final String interfaceMask) {
        final AddressMatcher mask;
        try {
            mask = getAddressMatcher(interfaceMask);
        } catch (Exception e) {
            return false;
        }
        return mask.match(address);
    }

    public static AddressHolder getAddressHolder(String address) {
        return getAddressHolder(address, -1);
    }

    public static AddressHolder getAddressHolder(String address, int defaultPort) {
        final int indexBracketStart = address.indexOf('[');
        final int indexBracketEnd = address.indexOf(']', indexBracketStart);
        final int indexColon = address.indexOf(':');
        final int lastIndexColon = address.lastIndexOf(':');
        final String host;
        final int port;
        if (indexColon > -1 && lastIndexColon > indexColon) {
            // IPv6
            if (indexBracketStart == 0 && indexBracketEnd > indexBracketStart
                    && lastIndexColon == indexBracketEnd + 1) {
                host = address.substring(indexBracketStart + 1, indexBracketEnd);
                port = Integer.parseInt(address.substring(lastIndexColon + 1));
            } else {
                host = address;
                port = defaultPort;
            }
        } else if (indexColon > 0 && indexColon == lastIndexColon) {
            host = address.substring(0, indexColon);
            port = Integer.parseInt(address.substring(indexColon + 1));
        } else {
            host = address;
            port = defaultPort;
        }
        return new AddressHolder(host, port);
    }

    public static boolean isIpAddress(String address) {
        try {
            return getAddressMatcher(address) != null;
        } catch (InvalidAddressException e) {
            return false;
        }
    }

    public static AddressMatcher getAddressMatcher(String address) {
        final AddressMatcher matcher;
        final int indexColon = address.indexOf(':');
        final int lastIndexColon = address.lastIndexOf(':');
        final int indexDot = address.indexOf('.');
        final int lastIndexDot = address.lastIndexOf('.');

        if (indexColon > -1 && lastIndexColon > indexColon) {
            if (indexDot == -1) {
                matcher = new Ip6AddressMatcher();
                parseIpv6(matcher, address);
            } else {
                // IPv4 mapped IPv6
                if (indexDot >= lastIndexDot) {
                    throw new InvalidAddressException(address);
                }
                final int lastIndexColon2 = address.lastIndexOf(':');
                final String host2 = address.substring(lastIndexColon2 + 1);
                matcher = new Ip4AddressMatcher();
                parseIpv4(matcher, host2);
            }
        } else if (indexDot > -1 && lastIndexDot > indexDot && indexColon == -1) {
            // IPv4
            matcher = new Ip4AddressMatcher();
            parseIpv4(matcher, address);
        } else {
            throw new InvalidAddressException(address);
        }
        return matcher;
    }

    private static void parseIpv4(AddressMatcher matcher, String address) {
        final String[] parts = address.split("\\.");
        if (parts.length != 4) {
            throw new InvalidAddressException(address);
        }
        matcher.setAddress(parts);
    }

    private static void parseIpv6(AddressMatcher matcher, String address) {
        if (address.indexOf('%') > -1) {
            String[] parts = address.split("\\%");
            address = parts[0];
            matcher.setScopeId(parts[1]);
        }
        final String[] parts = address.split("((?<=:)|(?=:))");
        final LinkedList<String> ipString = new LinkedList<String>();
        int count = 0;
        int mark = -1;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String nextPart = i < parts.length - 1 ? parts[i + 1] : null;
            if ("".equals(part)) {
                continue;
            }
            if (":".equals(part) && ":".equals(nextPart)) {
                if (mark != -1) {
                    throw new InvalidAddressException(address);
                }
                mark = count;
            } else if (!":".equals(part)) {
                count++;
                ipString.add(part);
            }
        }
        if (mark > -1) {
            final int remaining = (8 - count);
            for (int i = 0; i < remaining; i++) {
                ipString.add((i + mark), "0");
            }
        }
        if (ipString.size() != 8) {
            throw new InvalidAddressException(address);
        }
        matcher.setAddress(ipString.toArray(new String[0]));
    }

    // ----------------- UTILITY CLASSES ------------------

    public static class AddressHolder {
        public final String address;
        public final int port;

        public AddressHolder(final String address, final int port) {
            this.address = address;
            this.port = port;
        }
    }

    /**
     * http://docs.oracle.com/javase/1.5.0/docs/guide/net/ipv6_guide/index.html
     */

    public static abstract class AddressMatcher {
        protected final String[] address;

        protected AddressMatcher(final String[] address) {
            this.address = address;
        }

        public abstract boolean isIPv4();

        public abstract boolean isIPv6();

        public String getScopeId() {
            return null;
        }

        public void setScopeId(final String scopeId) {
            throw new UnsupportedOperationException();
        }

        public abstract void setAddress(String ip[]);

        protected final boolean match(final String[] mask, String[] input, int radix) {
            if (input != null && mask != null) {
                for (int i = 0; i < mask.length; i++) {
                    if (!doMatch(mask[i], input[i], radix)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        protected final boolean doMatch(final String mask, String input, int radix) {
            final int dashIndex = mask.indexOf('-');
            final int ipa = Integer.parseInt(input, radix);
            if ("*".equals(mask)) {
                return true;
            } else if (dashIndex != -1) {
                final int start = Integer.parseInt(mask.substring(0, dashIndex).trim(), radix);
                final int end = Integer.parseInt(mask.substring(dashIndex + 1).trim(), radix);
                if (ipa >= start && ipa <= end) {
                    return true;
                }
            } else {
                final int x = Integer.parseInt(mask, radix);
                if (x == ipa) {
                    return true;
                }
            }
            return false;
        }

        public abstract String getAddress();

        public abstract boolean match(AddressMatcher matcher);

        public boolean match(final String address) {
            try {
                return match(getAddressMatcher(address));
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName());
            sb.append('{');
            sb.append(getAddress());
            sb.append('}');
            return sb.toString();
        }
    }

    static class Ip4AddressMatcher extends AddressMatcher {
        public Ip4AddressMatcher() {
            super(new String[4]);  // d.d.d.d
        }

        public boolean isIPv4() {
            return true;
        }

        public boolean isIPv6() {
            return false;
        }

        public void setAddress(String ip[]) {
            for (int i = 0; i < ip.length; i++) {
                this.address[i] = ip[i];
            }
        }

        public boolean match(final AddressMatcher matcher) {
            if (matcher.isIPv6()) return false;
            final String[] mask = this.address;
            final String[] input = ((Ip4AddressMatcher) matcher).address;
            return match(mask, input, 10);
        }

        public String getAddress() {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < address.length; i++) {
                sb.append(address[i]);
                if (i != address.length - 1) {
                    sb.append('.');
                }
            }
            return sb.toString();
        }
    }

    static class Ip6AddressMatcher extends AddressMatcher {
        String scopeId;

        public Ip6AddressMatcher() {
            super(new String[8]);  // x:x:x:x:x:x:x:x%s
        }

        public boolean isIPv4() {
            return false;
        }

        public boolean isIPv6() {
            return true;
        }

        public String getScopeId() {
            return scopeId;
        }

        public void setScopeId(final String scopeId) {
            this.scopeId = scopeId;
        }

        public void setAddress(String ip[]) {
            for (int i = 0; i < ip.length; i++) {
                this.address[i] = ip[i];
            }
        }

        public boolean match(final AddressMatcher matcher) {
            if (matcher.isIPv4()) return false;
            final Ip6AddressMatcher a = (Ip6AddressMatcher) matcher;
            if (scopeId != null && !"*".equals(scopeId)) {
                if (!scopeId.equals(a.scopeId)) {
                    return false;
                }
            }
            final String[] mask = this.address;
            final String[] input = a.address;
            return match(mask, input, 16);
        }

        public String getAddress() {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < address.length; i++) {
                sb.append(address[i]);
                if (i != address.length - 1) {
                    sb.append(':');
                }
            }
            if (scopeId != null) {
                sb.append('%').append(scopeId);
            }
            return sb.toString();
        }
    }

    public static class InvalidAddressException extends IllegalArgumentException {
        public InvalidAddressException(final String s) {
            super("Illegal IP address format: " + s);
        }
    }
}
