package datest;

import java.io.Serializable;
import java.util.Objects;

public class TestPojo implements Serializable {
    private static final long serialVersionUID = 6492714073345165037L;
    private String field1;

    @Override
    public String toString() {
        return getField1();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final TestPojo testPojo = (TestPojo) o;
        return Objects.equals(getField1(), testPojo.getField1());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getField1());
    }

    public TestPojo(final String field1) {
        setField1(field1);
    }

    public String getField1() {
        return field1;
    }

    public void setField1(final String field1) {
        this.field1 = field1;
    }
}