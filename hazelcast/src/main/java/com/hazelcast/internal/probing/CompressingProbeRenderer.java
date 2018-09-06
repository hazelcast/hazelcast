package com.hazelcast.internal.probing;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public final class CompressingProbeRenderer implements ProbeRenderer {

    private static final ILogger LOGGER = Logger.getLogger(CompressingProbeRenderer.class);

    private static final int BINARY_FORMAT_VERSION = 1;

    private final DataOutputStream out;

    public CompressingProbeRenderer(OutputStream out) throws IOException {
        Deflater compressor = new Deflater(Deflater.BEST_SPEED);
        out.write(BINARY_FORMAT_VERSION);
        this.out = new DataOutputStream(new DeflaterOutputStream(out, compressor));
    }

    public void done() {
        try {
            out.close();
        } catch (IOException e) {
            LOGGER.fine("Exception while flushing stream.", e);
        }
    }

    @Override
    public void render(CharSequence key, long value) {
        try {
            writeKey(key);
            out.writeLong(value);
        } catch (Exception e) {
            LOGGER.fine("Failed to render metric.", e);
        }
    }

    /**
     * {@link DataOutputStream#writeUTF(String)} only works on {@link String} and
     * creates intermediate byte[]. The benefit of avoiding garbage is more
     * important than saving a few bytes though use of char encoding as the
     * {@link Deflater} will optimize re-occurring sequences a few extra bytes
     * presumably do not make much of a difference.
     */
    private void writeKey(CharSequence key) throws IOException {
        int len = key.length();
        out.writeShort(len);
        for (int i = 0; i < len; i++) {
            out.writeChar(key.charAt(i));
        }
    }

    public static void decompress(InputStream compressed, StringBuilder out) throws IOException {
        int version = compressed.read();
        if (version != BINARY_FORMAT_VERSION) {
            throw new UnsupportedOperationException(
                    "Compressed data has unknown binary format: " + version);
        }
        DataInputStream in = new DataInputStream(new InflaterInputStream(compressed));
        try {
            while (true) {
                int keyLength = in.readUnsignedShort();
                for (int i = 0; i < keyLength; i++) {
                    out.append(in.readChar());
                }
                out.append(' ');
                out.append(in.readLong());
                out.append('\n');
            }
        } catch (EOFException e) {
            // done
        } finally {
            in.close();
        }
    }
}
