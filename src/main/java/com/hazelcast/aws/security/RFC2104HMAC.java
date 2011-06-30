/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.aws.security;

import static com.hazelcast.aws.impl.Constants.SIGNATURE_METHOD;

import java.security.SignatureException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class RFC2104HMAC {

	public static String calculateRFC2104HMAC(String data, String key)
			throws SignatureException {
		String result = null;
		try {
			SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(),
					SIGNATURE_METHOD);
			Mac mac = Mac.getInstance(SIGNATURE_METHOD);
			mac.init(signingKey);
			byte[] rawSignature = mac.doFinal(data.getBytes());
			result = encode(rawSignature);
			result = result.trim();
		} catch (Exception e) {
			throw new SignatureException("Failed to generate HMAC : "
					+ e.getMessage());
		}
		return result;
	}

	static private final int LOOKUPLENGTH = 64;
	static private final int TWENTYFOURBITGROUP = 24;
	static private final int EIGHTBIT = 8;
	static private final int SIXTEENBIT = 16;
	static private final int SIGN = -128;
	static private final char PAD = '=';
	static final private char[] lookUpBase64Alphabet = new char[LOOKUPLENGTH];

	static {
		for (int i = 0; i <= 25; i++)
			lookUpBase64Alphabet[i] = (char) ('A' + i);

		for (int i = 26, j = 0; i <= 51; i++, j++)
			lookUpBase64Alphabet[i] = (char) ('a' + j);

		for (int i = 52, j = 0; i <= 61; i++, j++)
			lookUpBase64Alphabet[i] = (char) ('0' + j);
		lookUpBase64Alphabet[62] = (char) '+';
		lookUpBase64Alphabet[63] = (char) '/';

	}

	private static String encode(byte[] binaryData) {
		if (binaryData == null)
			return null;

		int lengthDataBits = binaryData.length * EIGHTBIT;
		if (lengthDataBits == 0) {
			return "";
		}

		int fewerThan24bits = lengthDataBits % TWENTYFOURBITGROUP;
		int numberTriplets = lengthDataBits / TWENTYFOURBITGROUP;
		int numberQuartet = fewerThan24bits != 0 ? numberTriplets + 1
				: numberTriplets;
		char encodedData[] = null;

		encodedData = new char[numberQuartet * 4];

		byte k = 0, l = 0, b1 = 0, b2 = 0, b3 = 0;

		int encodedIndex = 0;
		int dataIndex = 0;

		for (int i = 0; i < numberTriplets; i++) {
			b1 = binaryData[dataIndex++];
			b2 = binaryData[dataIndex++];
			b3 = binaryData[dataIndex++];

			l = (byte) (b2 & 0x0f);
			k = (byte) (b1 & 0x03);

			byte val1 = ((b1 & SIGN) == 0) ? (byte) (b1 >> 2)
					: (byte) ((b1) >> 2 ^ 0xc0);

			byte val2 = ((b2 & SIGN) == 0) ? (byte) (b2 >> 4)
					: (byte) ((b2) >> 4 ^ 0xf0);
			byte val3 = ((b3 & SIGN) == 0) ? (byte) (b3 >> 6)
					: (byte) ((b3) >> 6 ^ 0xfc);

			encodedData[encodedIndex++] = lookUpBase64Alphabet[val1];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[val2 | (k << 4)];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[(l << 2) | val3];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[b3 & 0x3f];
		}

		// form integral number of 6-bit groups
		if (fewerThan24bits == EIGHTBIT) {
			b1 = binaryData[dataIndex];
			k = (byte) (b1 & 0x03);
			byte val1 = ((b1 & SIGN) == 0) ? (byte) (b1 >> 2)
					: (byte) ((b1) >> 2 ^ 0xc0);
			encodedData[encodedIndex++] = lookUpBase64Alphabet[val1];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[k << 4];
			encodedData[encodedIndex++] = PAD;
			encodedData[encodedIndex++] = PAD;
		} else if (fewerThan24bits == SIXTEENBIT) {
			b1 = binaryData[dataIndex];
			b2 = binaryData[dataIndex + 1];
			l = (byte) (b2 & 0x0f);
			k = (byte) (b1 & 0x03);

			byte val1 = ((b1 & SIGN) == 0) ? (byte) (b1 >> 2)
					: (byte) ((b1) >> 2 ^ 0xc0);
			byte val2 = ((b2 & SIGN) == 0) ? (byte) (b2 >> 4)
					: (byte) ((b2) >> 4 ^ 0xf0);

			encodedData[encodedIndex++] = lookUpBase64Alphabet[val1];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[val2 | (k << 4)];
			encodedData[encodedIndex++] = lookUpBase64Alphabet[l << 2];
			encodedData[encodedIndex++] = PAD;
		}

		return new String(encodedData);
	}
}
