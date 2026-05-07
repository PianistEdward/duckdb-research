package demo.duckdb.binrecord;

import java.util.Arrays;

record SignalMeta(int index, SignalType type, int[] dimensions, String name, String formula) {
    boolean isArray() {
        return dimensions.length > 0;
    }

    int elementCount() throws ProtocolException {
        if (!isArray()) {
            return 1;
        }
        long count = 1;
        for (int dimension : dimensions) {
            if (dimension <= 0) {
                throw new ProtocolException("Array dimension must be positive for signal " + name);
            }
            count *= dimension;
            if (count > Integer.MAX_VALUE) {
                throw new ProtocolException("Array element count overflows int for signal " + name);
            }
        }
        return (int) count;
    }

    String dimensionsJson() {
        return Arrays.toString(dimensions);
    }
}
