package demo.duckdb.binrecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class SampleBinaryRecordWriter {
    private SampleBinaryRecordWriter() {
    }

    static Path writeSample(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        BinaryRecordSchema schema = sampleSchema();
        try (var out = Files.newOutputStream(path)) {
            writeLong(out, schema.version());
            writeLong(out, schema.signalCount());
            for (SignalMeta signal : schema.signals()) {
                writeSignalMeta(out, signal);
            }

            writeFrame(out, schema.signalCount(), 1_000L, new Object[] {null, 12.5d, 3.3f, "warmup", new double[] {1.0, 2.0, 3.0}});
            writeFrame(out, schema.signalCount(), 2_000L, new Object[] {null, 15.0d, null, "stable", new double[] {1.5, 2.5, 3.5}});
            writeFrame(out, schema.signalCount(), 3_000L, new Object[] {null, 9.75d, 3.1f, null, null});

            writeLong(out, BinaryRecordReader.EOF_MARKER);
        }
        return path;
    }

    private static BinaryRecordSchema sampleSchema() {
        return new BinaryRecordSchema(BinaryRecordReader.SUPPORTED_VERSION, List.of(
                new SignalMeta(0, SignalType.UINT64, new int[0], "time_ns", "sample timestamp"),
                new SignalMeta(1, SignalType.DOUBLE, new int[0], "speed", "m/s"),
                new SignalMeta(2, SignalType.FLOAT, new int[0], "voltage", "V"),
                new SignalMeta(3, SignalType.STRING, new int[0], "status", "state label"),
                new SignalMeta(4, SignalType.DOUBLE, new int[] {3}, "spectrum", "three-bin spectrum")
        ));
    }

    private static void writeSignalMeta(java.io.OutputStream out, SignalMeta signal) throws IOException {
        out.write(signal.type().code);
        writeInt(out, signal.dimensions().length);
        for (int dimension : signal.dimensions()) {
            writeInt(out, dimension);
        }
        byte[] name = signal.name().getBytes(StandardCharsets.UTF_8);
        writeLong(out, name.length);
        out.write(name);
        byte[] formula = signal.formula().getBytes(StandardCharsets.UTF_8);
        writeInt(out, formula.length);
        out.write(formula);
    }

    private static void writeFrame(java.io.OutputStream out, int signalCount, long timeNs, Object[] values) throws IOException {
        byte[] bitmap = new byte[(signalCount + 7) / 8];
        setBit(bitmap, 0);
        for (int i = 1; i < signalCount; i++) {
            if (values[i] != null) {
                setBit(bitmap, i);
            }
        }
        out.write(bitmap);
        writeLong(out, timeNs);

        if (values[1] != null) {
            writeDouble(out, (Double) values[1]);
        }
        if (values[2] != null) {
            writeFloat(out, (Float) values[2]);
        }
        if (values[3] != null) {
            writeString(out, (String) values[3]);
        }
        if (values[4] != null) {
            for (double value : (double[]) values[4]) {
                writeDouble(out, value);
            }
        }
    }

    private static void setBit(byte[] bitmap, int signalIndex) {
        bitmap[signalIndex / 8] = (byte) (bitmap[signalIndex / 8] | (1 << (signalIndex % 8)));
    }

    private static void writeString(java.io.OutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeLong(out, bytes.length);
        out.write(bytes);
    }

    private static void writeInt(java.io.OutputStream out, int value) throws IOException {
        out.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
    }

    private static void writeLong(java.io.OutputStream out, long value) throws IOException {
        out.write(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
    }

    private static void writeFloat(java.io.OutputStream out, float value) throws IOException {
        out.write(ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array());
    }

    private static void writeDouble(java.io.OutputStream out, double value) throws IOException {
        out.write(ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array());
    }
}
