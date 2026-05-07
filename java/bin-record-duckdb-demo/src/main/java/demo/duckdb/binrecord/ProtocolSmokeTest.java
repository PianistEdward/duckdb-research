package demo.duckdb.binrecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * Minimal no-framework smoke test so the demo can be verified with only Maven
 * and the repository-local duckdb_jdbc jar.
 */
public final class ProtocolSmokeTest {
    private ProtocolSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of("target/smoke-test");
        Files.createDirectories(dir);
        Path bin = dir.resolve("sample.bin");
        Path db = dir.resolve("sample.duckdb");
        Files.deleteIfExists(db);

        SampleBinaryRecordWriter.writeSample(bin);
        BinaryRecordImportResult result = new DuckDbImporter().importFile(bin, db, "sim_record");
        assertEquals(3L, result.frameCount(), "frame count");
        assertEquals(5, result.schema().signalCount(), "signal count");

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:" + db.toAbsolutePath());
             ResultSet rs = connection.createStatement()
                     .executeQuery("SELECT COUNT(*), COUNT(voltage), COUNT(status), SUM(speed) FROM sim_record")) {
            if (!rs.next()) {
                throw new AssertionError("Expected aggregate row");
            }
            assertEquals(3L, rs.getLong(1), "row count");
            assertEquals(2L, rs.getLong(2), "non-null voltage count");
            assertEquals(2L, rs.getLong(3), "non-null status count");
            assertEquals(37.25d, rs.getDouble(4), "speed sum");
        }

        assertProtocolFailure(dir.resolve("bad-version.bin"), ProtocolSmokeTest::writeBadVersion);
        assertProtocolFailure(dir.resolve("variable-array.bin"), ProtocolSmokeTest::writeVariableLengthArraySchema);
        assertImportFailureForTruncatedFile(bin, dir.resolve("truncated.bin"), dir.resolve("truncated.duckdb"));

        System.out.println("ProtocolSmokeTest passed");
    }

    private static void assertProtocolFailure(Path path, ThrowingPathWriter writer) throws Exception {
        writer.write(path);
        try (BinaryRecordReader reader = new BinaryRecordReader(path)) {
            reader.readSchema();
            throw new AssertionError("Expected ProtocolException for " + path.getFileName());
        } catch (ProtocolException expected) {
            // Expected: malformed protocol input should be rejected before import.
        }
    }

    private static void assertImportFailureForTruncatedFile(Path source, Path truncated, Path db) throws Exception {
        byte[] bytes = Files.readAllBytes(source);
        Files.write(truncated, java.util.Arrays.copyOf(bytes, bytes.length - 4));
        try {
            new DuckDbImporter().importFile(truncated, db, "bad_record");
            throw new AssertionError("Expected ProtocolException for truncated file");
        } catch (ProtocolException expected) {
            // Expected: EOF marker is incomplete after the last full frame.
        }
    }

    private static void writeBadVersion(Path path) throws Exception {
        try (var out = Files.newOutputStream(path)) {
            writeLong(out, 99L);
            writeLong(out, 0L);
        }
    }

    private static void writeVariableLengthArraySchema(Path path) throws Exception {
        try (var out = Files.newOutputStream(path)) {
            writeLong(out, BinaryRecordReader.SUPPORTED_VERSION);
            writeLong(out, 2L);
            writeSignalMeta(out, SignalType.UINT64, new int[0], "time_ns", "");
            writeSignalMeta(out, SignalType.STRING, new int[] {2}, "labels", "");
            writeLong(out, BinaryRecordReader.EOF_MARKER);
        }
    }

    private static void writeSignalMeta(java.io.OutputStream out, SignalType type, int[] dimensions, String name,
            String formula) throws Exception {
        out.write(type.code);
        writeInt(out, dimensions.length);
        for (int dimension : dimensions) {
            writeInt(out, dimension);
        }
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        writeLong(out, nameBytes.length);
        out.write(nameBytes);
        byte[] formulaBytes = formula.getBytes(StandardCharsets.UTF_8);
        writeInt(out, formulaBytes.length);
        out.write(formulaBytes);
    }

    private static void writeInt(java.io.OutputStream out, int value) throws Exception {
        out.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private static void writeLong(java.io.OutputStream out, long value) throws Exception {
        out.write(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }

    private static void assertEquals(long expected, long actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertEquals(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertEquals(double expected, double actual, String label) {
        if (Math.abs(expected - actual) > 0.000001d) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    @FunctionalInterface
    private interface ThrowingPathWriter {
        void write(Path path) throws Exception;
    }
}
