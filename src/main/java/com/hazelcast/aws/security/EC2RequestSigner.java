package com.hazelcast.aws.security;

import com.hazelcast.aws.impl.Constants;
import com.hazelcast.aws.utility.AwsURLEncoder;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.util.QuickMath;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by igmar on 03/11/14.
 */
public class EC2RequestSigner {
    private static final String API_TERMINATOR = "aws4_request";

    private final AwsConfig config;
    private String service;
    private final String timestamp;
    private String signature;
    private Map<String, String> attributes;
    private String canonicalRequest;
    private String stringToSign;
    private byte[] signingKey;

    public EC2RequestSigner(AwsConfig config, final String timeStamp) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        if (timeStamp == null) {
            throw new IllegalArgumentException("timeStamp cannot be null");
        }
        this.config = config;
        this.timestamp = timeStamp;
        this.service = null;
    }

    public String getCredentialScope() {
        // datestamp/region/service/API_TERMINATOR
        final String dateStamp = timestamp.substring(0, 8);
        return String.format("%s/%s/%s/%s", dateStamp, config.getRegion(), this.service, API_TERMINATOR);
    }

    public String getSignedHeaders() {
        return "host";
    }

    public String sign(String service, Map<String, String> attributes) {
        if (service == null) {
            throw new IllegalArgumentException("service cannot be null");
        }
        if (attributes == null) {
            throw new IllegalArgumentException("attributes cannot be null");
        }

        this.service = service;
        this.attributes = attributes;
        canonicalRequest = getCanonicalizedRequest();
        stringToSign = createStringToSign(canonicalRequest);
        signingKey = deriveSigningKey();
        this.signature = createSignature(stringToSign, signingKey);

        return this.signature;
    }


    /* Task 1 */
    private String getCanonicalizedRequest() {
        StringBuilder sb = new StringBuilder();

        // Method + \n
        sb.append(Constants.GET).append("\n");
        // canonical_url + \n
        sb.append("/").append("\n");
        // canonical_querystring + \n
        sb.append(getCanonicalizedQueryString(this.attributes)).append("\n");
        // canonical_headers + \n
        sb.append(getCanonicalHeaders()).append("\n");
        // signed headers + \n
        sb.append(getSignedHeaders()).append("\n");
        // payload hash
        sb.append(sha256Hashhex(""));

        return sb.toString();
    }

    /* Task 2 */
    private String createStringToSign(String canonicalRequest) {
        StringBuilder sb = new StringBuilder();
        // Algorithm
        sb.append(Constants.SIGNATURE_METHOD_V4).append("\n");
        // requestDate
        sb.append(timestamp).append("\n");
        // CredentialScope
        sb.append(getCredentialScope()).append("\n");
        // HashedCanonicalRequest
        sb.append(sha256Hashhex(canonicalRequest));

        return sb.toString();
    }

    /* Task 3 */
    private byte[] deriveSigningKey() {
        final String signKey = config.getSecretKey();
        final String dateStamp = timestamp.substring(0, 8);
        // This is derived from
        // http://docs.com.hazelcast.aws.security.amazon.com/general/latest/gr/signature-v4-examples.html
        // #signature-v4-examples-python

        try {
            final String key = "AWS4" + signKey;
            final Mac mDate = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skDate = new SecretKeySpec(key.getBytes(), "HmacSHA256");
            mDate.init(skDate);
            final byte[] kDate = mDate.doFinal(dateStamp.getBytes());

            final Mac mRegion = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skRegion = new SecretKeySpec(kDate, "HmacSHA256");
            mRegion.init(skRegion);
            final byte[] kRegion = mRegion.doFinal(config.getRegion().getBytes());

            final Mac mService = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skService = new SecretKeySpec(kRegion, "HmacSHA256");
            mService.init(skService);
            final byte[] kService = mService.doFinal(this.service.getBytes());

            final Mac mSigning = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skSigning = new SecretKeySpec(kService, "HmacSHA256");
            mSigning.init(skSigning);
            final byte[] kSigning = mSigning.doFinal("aws4_request".getBytes());

            return kSigning;
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        }
    }

    private String createSignature(String stringToSign, byte[] signingKey) {
        byte[] signature = null;
        try {
            final Mac signMac = Mac.getInstance("HmacSHA256");
            final SecretKeySpec signKS = new SecretKeySpec(signingKey, "HmacSHA256");
            signMac.init(signKS);
            signature = signMac.doFinal(stringToSign.getBytes());
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        }

        return QuickMath.bytesToHex(signature);
    }


    protected String getCanonicalHeaders() {
        return String.format("host:%s\n", config.getHostHeader());
    }

    public String getCanonicalizedQueryString(Map<String, String> attributes) {
        List<String> components = getListOfEntries(attributes);
        Collections.sort(components);
        return getCanonicalizedQueryString(components);
    }


    protected String getCanonicalizedQueryString(List<String> list) {
        Iterator<String> it = list.iterator();
        StringBuilder result = new StringBuilder(it.next());
        while (it.hasNext()) {
            result.append("&").append(it.next());
        }
        return result.toString();
    }

    protected void addComponents(List<String> components, Map<String, String> attributes, String key) {
        components.add(AwsURLEncoder.urlEncode(key) + "=" + AwsURLEncoder.urlEncode(attributes.get(key)));
    }

    protected List<String> getListOfEntries(Map<String, String> entries) {
        List<String> components = new ArrayList<String>();
        for (String key : entries.keySet()) {
            addComponents(components, entries, key);
        }
        return components;
    }



    private String sha256Hashhex(final String in) {
        String payloadHash = "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(in.getBytes("UTF-8"));
            byte[] digest = md.digest();
            payloadHash = QuickMath.bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
        return payloadHash;
    }
}
