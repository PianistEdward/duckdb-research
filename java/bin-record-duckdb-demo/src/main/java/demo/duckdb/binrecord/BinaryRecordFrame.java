package demo.duckdb.binrecord;

import java.util.Arrays;

record BinaryRecordFrame(long timeNs, boolean[] present, Object[] values) {
    BinaryRecordFrame {
        present = Arrays.copyOf(present, present.length);
        values = Arrays.copyOf(values, values.length);
    }

    boolean hasValue(int index) {
        return present[index];
    }

    Object value(int index) {
        return values[index];
    }
}
