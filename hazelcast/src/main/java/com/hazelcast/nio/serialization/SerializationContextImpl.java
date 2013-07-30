package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataOutput;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
* @author mdogan 7/29/13
*/
final class SerializationContextImpl implements SerializationContext {

    final int version;
    final Map<Integer, PortableContext> portableContextMap;
    final SerializationServiceImpl serializationService;

    SerializationContextImpl(SerializationServiceImpl serializationService, Collection<Integer> portableFactories, int version) {
        this.serializationService = serializationService;
        this.version = version;
        final Map<Integer, PortableContext> portableMap = new HashMap<Integer, PortableContext>();
        for (int factoryId : portableFactories) {
            portableMap.put(factoryId, new PortableContext());
        }
        portableContextMap = portableMap; // do not modify!
    }

    public ClassDefinition lookup(int factoryId, int classId) {
        return getPortableContext(factoryId).lookup(classId, version);
    }

    public ClassDefinition lookup(int factoryId, int classId, int version) {
        return getPortableContext(factoryId).lookup(classId, version);
    }

    public ClassDefinition createClassDefinition(int factoryId, final byte[] compressedBinary) throws IOException {
        return getPortableContext(factoryId).createClassDefinition(compressedBinary);
    }

    public ClassDefinition registerClassDefinition(final ClassDefinition cd) {
        return getPortableContext(cd.getFactoryId()).registerClassDefinition(cd);
    }

    public ClassDefinition lookupOrRegisterClassDefinition(Portable p) throws IOException {
        ClassDefinition cd = lookup(p.getFactoryId(), p.getClassId());
        if (cd == null) {
            ClassDefinitionWriter classDefinitionWriter = new ClassDefinitionWriter(this, p.getFactoryId(), p.getClassId());
            p.writePortable(classDefinitionWriter);
            cd = classDefinitionWriter.registerAndGet();
        }
        return cd;
    }

    private void registerNestedDefinitions(ClassDefinitionImpl cd) {
        Collection<ClassDefinition> nestedDefinitions = cd.getNestedClassDefinitions();
        for (ClassDefinition classDefinition : nestedDefinitions) {
            final ClassDefinitionImpl nestedCD = (ClassDefinitionImpl) classDefinition;
            registerClassDefinition(nestedCD);
            registerNestedDefinitions(nestedCD);
        }
    }

    private PortableContext getPortableContext(int factoryId) {
        final PortableContext ctx = portableContextMap.get(factoryId);
        if (ctx == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factoryId: " + factoryId);
        }
        return ctx;
    }

    public int getVersion() {
        return version;
    }

    private class PortableContext {

        final ConcurrentMap<Long, ClassDefinitionImpl> versionedDefinitions = new ConcurrentHashMap<Long, ClassDefinitionImpl>();

        ClassDefinition lookup(int classId, int version) {
            return versionedDefinitions.get(combineToLong(classId, version));
        }

        ClassDefinition createClassDefinition(byte[] compressedBinary) throws IOException {
            if (compressedBinary == null || compressedBinary.length == 0) {
                throw new IOException("Illegal class-definition binary! ");
            }
            final BufferObjectDataOutput out = serializationService.pop();
            final byte[] binary;
            try {
                decompress(compressedBinary, out);
                binary = out.toByteArray();
            } finally {
                serializationService.push(out);
            }
            final ClassDefinitionImpl cd = new ClassDefinitionImpl();
            cd.readData(serializationService.createObjectDataInput(binary));
            cd.setBinary(compressedBinary);
            final ClassDefinitionImpl currentCD = versionedDefinitions.putIfAbsent(combineToLong(cd.classId, getVersion()), cd);
            if (currentCD == null) {
                registerNestedDefinitions(cd);
                return cd;
            } else {
                return currentCD;
            }
        }

        ClassDefinition registerClassDefinition(ClassDefinition cd) {
            if (cd == null) return null;
            final ClassDefinitionImpl cdImpl = (ClassDefinitionImpl) cd;
            if (cdImpl.getVersion() < 0) {
                cdImpl.version = getVersion();
            }
            if (cdImpl.getBinary() == null) {
                final BufferObjectDataOutput out = serializationService.pop();
                try {
                    cdImpl.writeData(out);
                    final byte[] binary = out.toByteArray();
                    out.clear();
                    compress(binary, out);
                    cdImpl.setBinary(out.toByteArray());
                } catch (IOException e) {
                    throw new HazelcastSerializationException(e);
                } finally {
                    serializationService.push(out);
                }
            }
            final long versionedClassId = combineToLong(cdImpl.getClassId(), cdImpl.getVersion());
            final ClassDefinitionImpl currentClassDef = versionedDefinitions.putIfAbsent(versionedClassId, cdImpl);
            if (currentClassDef == null) {
                registerNestedDefinitions(cdImpl);
                return cd;
            }
            return currentClassDef;
        }
    }

    static void compress(byte[] input, DataOutput out) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
        deflater.setStrategy(Deflater.FILTERED);
        deflater.setInput(input);
        deflater.finish();
        byte[] buf = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buf);
            out.write(buf, 0, count);
        }
        deflater.end();
    }

    static void decompress(byte[] compressedData, DataOutput out) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);
        byte[] buf = new byte[1024];
        while (!inflater.finished()) {
            try {
                int count = inflater.inflate(buf);
                out.write(buf, 0, count);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }
        }
        inflater.end();
    }

    static long combineToLong(int x, int y) {
        return ((long) x << 32) | ((long) y & 0xFFFFFFFL);
    }

    static int extractInt(long value, boolean lowerBits) {
        return (lowerBits) ? (int) value : (int) (value >> 32);
    }
}
