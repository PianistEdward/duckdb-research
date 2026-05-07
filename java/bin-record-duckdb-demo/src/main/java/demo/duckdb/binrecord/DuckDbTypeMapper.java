package demo.duckdb.binrecord;

import java.sql.SQLException;
import org.duckdb.DuckDBAppender;

final class DuckDbTypeMapper {
    private DuckDbTypeMapper() {
    }

    static String duckDbType(SignalMeta signal) {
        String scalarType = scalarDuckDbType(signal.type());
        if (!signal.isArray()) {
            return scalarType;
        }
        // Current protocol arrays have fixed-length elements. Demo stores them as DuckDB LIST.
        return scalarType + "[]";
    }

    static void appendValue(DuckDBAppender appender, SignalMeta signal, Object value) throws SQLException {
        if (signal.isArray()) {
            appendArray(appender, signal, value);
            return;
        }
        appendScalar(appender, signal.type(), value);
    }

    private static String scalarDuckDbType(SignalType type) {
        return switch (type) {
            case UINT64 -> "UBIGINT";
            case UINT32 -> "UINTEGER";
            case UINT16 -> "USMALLINT";
            case UINT8 -> "UTINYINT";
            case INT64, ENUM -> "BIGINT";
            case INT32 -> "INTEGER";
            case INT16 -> "SMALLINT";
            case INT8 -> "TINYINT";
            case BOOL -> "BOOLEAN";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case STRING -> "VARCHAR";
            case BINARY -> "BLOB";
            // Appender struct writing is possible, but arrays keep the demo compact and visible in SQL.
            case COMPLEX32, COMPLEX64 -> "DOUBLE[]";
        };
    }

    private static void appendScalar(DuckDBAppender appender, SignalType type, Object value) throws SQLException {
        switch (type) {
            // duckdb_jdbc maps BigInteger to HUGEINT/UHUGEINT. For UBIGINT columns,
            // append the raw 64-bit value; DuckDB stores the same bit pattern.
            case UINT64 -> appender.append(parseUnsignedLongBits(value));
            case UINT32, UINT16 -> appender.append(((Number) value).longValue());
            case UINT8 -> appender.append(((Number) value).shortValue());
            case INT64, ENUM -> appender.append(((Number) value).longValue());
            case INT32 -> appender.append(((Number) value).intValue());
            case INT16 -> appender.append(((Number) value).shortValue());
            case INT8 -> appender.append(((Number) value).byteValue());
            case BOOL -> appender.append((Boolean) value);
            case FLOAT -> appender.append(((Number) value).floatValue());
            case DOUBLE -> appender.append(((Number) value).doubleValue());
            case STRING -> appender.append((String) value);
            case BINARY -> appender.appendByteArray((byte[]) value);
            case COMPLEX32, COMPLEX64 -> appender.append(((ComplexValue) value).asArray());
        }
    }

    private static void appendArray(DuckDBAppender appender, SignalMeta signal, Object value) throws SQLException {
        switch (signal.type()) {
            case UINT64 -> {
                String[] raw = (String[]) value;
                long[] converted = new long[raw.length];
                for (int i = 0; i < raw.length; i++) {
                    converted[i] = Long.parseUnsignedLong(raw[i]);
                }
                appender.append(converted);
            }
            case UINT32 -> appender.append((long[]) value);
            case UINT16 -> appender.append(toIntArray((int[]) value));
            case UINT8 -> appender.append(toShortList((short[]) value));
            case INT64, ENUM -> appender.append((long[]) value);
            case INT32 -> appender.append((int[]) value);
            case INT16 -> appender.append((short[]) value);
            case INT8 -> appender.append((byte[]) value);
            case BOOL -> appender.append((boolean[]) value);
            case FLOAT -> appender.append((float[]) value);
            case DOUBLE -> appender.append((double[]) value);
            case COMPLEX32, COMPLEX64 -> appender.append((double[][]) value);
            case STRING, BINARY -> throw new SQLException("Current protocol rejects variable-length arrays: " + signal.name());
        }
    }

    private static int[] toIntArray(int[] values) {
        return values;
    }

    private static java.util.List<Short> toShortList(short[] values) {
        java.util.ArrayList<Short> result = new java.util.ArrayList<>(values.length);
        for (short value : values) {
            result.add(value);
        }
        return result;
    }

    private static long parseUnsignedLongBits(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseUnsignedLong(value.toString());
    }
}
