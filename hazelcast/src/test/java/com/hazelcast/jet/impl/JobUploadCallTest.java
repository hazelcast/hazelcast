package com.hazelcast.jet.impl;

import com.hazelcast.jet.JetException;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JobUploadCallTest {

    @Test
    public void testFindFileNameWithoutExtension() {
        JobUploadCall jobUploadCall = new JobUploadCall();

        String expectedFileName = "foo";
        Path jarPath = Paths.get("/mnt/foo.jar");
        String fileNameWithoutExtension = jobUploadCall.findFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        Path jarPath1 = Paths.get("/mnt/foo");
        assertThrows(JetException.class, () -> jobUploadCall.findFileNameWithoutExtension(jarPath1));

        jarPath = Paths.get("foo.jar");
        fileNameWithoutExtension = jobUploadCall.findFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        Path jarPath2 = Paths.get("foo");
        assertThrows(JetException.class, () -> jobUploadCall.findFileNameWithoutExtension(jarPath2));
    }
}

