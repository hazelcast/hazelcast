package example.serialization;

import java.io.IOException;
import java.io.Serializable;

public class TestDeserialized implements Serializable {
    private static final long serialVersionUID = 1L;
    public static volatile boolean IS_DESERIALIZED = false;

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        IS_DESERIALIZED = true;
    }
}