package demo.duckdb.binrecord;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class BinaryRecordReader implements Closeable {
    static final long SUPPORTED_VERSION = 1L;
    static final long EOF_MARKER = 0xDEADBEEFDEADBEEFL;
    private static final int MAX_SIGNALS = 1024;
    private static final int MAX_DIMENSIONS = 8;
    private static final long MAX_TEXT_BYTES = 1L << 20;
    private static final long MAX_BLOB_BYTES = 64L << 20;

    private final SeekableByteChannel channel;
    private final LittleEndianData in;
    private BinaryRecordSchema schema;

    BinaryRecordReader(Path path) throws IOException {
        this.channel = Files.newByteChannel(path, StandardOpenOption.READ);
        this.in = new LittleEndianData(channel);
    }

    BinaryRecordSchema readSchema() throws IOException, ProtocolException {
        try {
            long version = in.readInt64();
            if (version != SUPPORTED_VERSION) {
                throw new ProtocolException("Unsupported protocol version: " + Long.toUnsignedString(version));
            }

            long signalCountUnsigned = in.readInt64();
            if (signalCountUnsigned <= 0 || signalCountUnsigned > MAX_SIGNALS) {
                throw new ProtocolException("Invalid signal_count: " + Long.toUnsignedString(signalCountUnsigned));
            }
            int signalCount = Math.toIntExact(signalCountUnsigned);

            List<SignalMeta> signals = new ArrayList<>(signalCount);
            for (int i = 0; i < signalCount; i++) {
                signals.add(readSignalMeta(i));
            }

            BinaryRecordSchema result = new BinaryRecordSchema(version, signals);
            validateSchema(result);
            this.schema = result;
            return result;
        } catch (EOFException error) {
            throw new ProtocolException("File ended while reading header", error);
        }
    }

    BinaryRecordFrame readNextFrameOrNull() throws IOException, ProtocolException {
        try {
            ensureSchema();
            if (in.remaining() < Long.BYTES) {
                throw new ProtocolException("EOF marker is missing or truncated");
            }

            long frameStart = in.position();
            long markerCandidate = in.readInt64();
            if (markerCandidate == EOF_MARKER && in.position() == in.size()) {
                return null;
            }

            // Current protocol has no frame_size, so EOF is only checked at frame boundaries.
            // Rewind the 8-byte probe and parse it as the bitmap prefix for the next frame.
            in.position(frameStart);
            int bitmapSize = (schema.signalCount() + 7) / 8;
            byte[] bitmap = in.readBytes(bitmapSize);
            boolean[] present = decodeBitmap(bitmap, schema.signalCount());
            if (!present[0]) {
                throw new ProtocolException("Frame at offset " + frameStart + " has bit 0 unset");
            }

            Object[] values = new Object[schema.signalCount()];
            long timeNs = in.readInt64();
            values[0] = Long.toUnsignedString(timeNs);

            for (int i = 1; i < schema.signalCount(); i++) {
                if (!present[i]) {
                    continue;
                }
                values[i] = readSignalValue(schema.signal(i));
            }
            return new BinaryRecordFrame(timeNs, present, values);
        } catch (EOFException error) {
            throw new ProtocolException("File ended while reading frame", error);
        }
    }

    private SignalMeta readSignalMeta(int index) throws IOException, ProtocolException {
        SignalType type = SignalType.fromCode(in.readUnsignedByte());
        long dimensionCountUnsigned = in.readUInt32AsLong();
        if (dimensionCountUnsigned > MAX_DIMENSIONS) {
            throw new ProtocolException("dimension_count too large for signal " + index);
        }
        int[] dimensions = new int[(int) dimensionCountUnsigned];
        for (int i = 0; i < dimensions.length; i++) {
            long dimension = in.readUInt32AsLong();
            if (dimension <= 0 || dimension > Integer.MAX_VALUE) {
                throw new ProtocolException("Invalid dimension " + dimension + " for signal " + index);
            }
            dimensions[i] = (int) dimension;
        }

        long nameLength = in.readInt64();
        validateLength(nameLength, MAX_TEXT_BYTES, "name_length for signal " + index);
        String name = in.readUtf8(nameLength, "name_length for signal " + index);
        long formulaLength = in.readUInt32AsLong();
        validateLength(formulaLength, MAX_TEXT_BYTES, "formula_length for signal " + index);
        String formula = in.readUtf8(formulaLength, "formula_length for signal " + index);
        return new SignalMeta(index, type, dimensions, name, formula);
    }

    private void validateSchema(BinaryRecordSchema schema) throws ProtocolException {
        SignalMeta time = schema.signal(0);
        if (time.type() != SignalType.UINT64 || time.isArray()) {
            throw new ProtocolException("Signal 0 must be scalar kUint64 because frames store time as fixed uint64");
        }
        for (SignalMeta signal : schema.signals()) {
            if (signal.name().isBlank()) {
                throw new ProtocolException("Signal name cannot be blank at index " + signal.index());
            }
            if (signal.isArray() && !signal.type().isArraySupportedByCurrentProtocol()) {
                throw new ProtocolException("Current protocol rejects variable-length arrays: " + signal.name());
            }
            signal.elementCount();
        }
    }

    private Object readSignalValue(SignalMeta signal) throws IOException, ProtocolException {
        if (signal.isArray()) {
            return readArray(signal);
        }
        return readScalar(signal.type());
    }

    private Object readArray(SignalMeta signal) throws IOException, ProtocolException {
        int elementCount = signal.elementCount();
        return switch (signal.type()) {
            case UINT64 -> {
                String[] values = new String[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = Long.toUnsignedString(in.readInt64());
                }
                yield values;
            }
            case UINT32 -> {
                long[] values = new long[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readUInt32AsLong();
                }
                yield values;
            }
            case UINT16 -> {
                int[] values = new int[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readUInt16AsInt();
                }
                yield values;
            }
            case UINT8 -> {
                short[] values = new short[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = (short) in.readUnsignedByte();
                }
                yield values;
            }
            case INT64, ENUM -> {
                long[] values = new long[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readInt64();
                }
                yield values;
            }
            case INT32 -> {
                int[] values = new int[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readInt32();
                }
                yield values;
            }
            case INT16 -> {
                short[] values = new short[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readInt16();
                }
                yield values;
            }
            case INT8 -> {
                byte[] values = new byte[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = (byte) in.readInt8();
                }
                yield values;
            }
            case BOOL -> {
                boolean[] values = new boolean[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readUnsignedByte() != 0;
                }
                yield values;
            }
            case FLOAT -> {
                float[] values = new float[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readFloat();
                }
                yield values;
            }
            case DOUBLE -> {
                double[] values = new double[elementCount];
                for (int i = 0; i < elementCount; i++) {
                    values[i] = in.readDouble();
                }
                yield values;
            }
            case COMPLEX32, COMPLEX64 -> {
                double[][] values = new double[elementCount][2];
                for (int i = 0; i < elementCount; i++) {
                    ComplexValue complex = (ComplexValue) readScalar(signal.type());
                    values[i] = complex.asArray();
                }
                yield values;
            }
            case STRING, BINARY -> throw new ProtocolException("Variable-length arrays are not allowed: " + signal.name());
        };
    }

    private Object readScalar(SignalType type) throws IOException, ProtocolException {
        return switch (type) {
            case UINT64 -> Long.toUnsignedString(in.readInt64());
            case UINT32 -> in.readUInt32AsLong();
            case UINT16 -> in.readUInt16AsInt();
            case UINT8 -> (short) in.readUnsignedByte();
            case INT64, ENUM -> in.readInt64();
            case INT32 -> in.readInt32();
            case INT16 -> in.readInt16();
            case INT8 -> (byte) in.readInt8();
            case BOOL -> in.readUnsignedByte() != 0;
            case FLOAT -> in.readFloat();
            case DOUBLE -> in.readDouble();
            case STRING -> readString();
            case BINARY -> readBlob();
            case COMPLEX32 -> new ComplexValue(in.readFloat(), in.readFloat());
            case COMPLEX64 -> new ComplexValue(in.readDouble(), in.readDouble());
        };
    }

    private String readString() throws IOException, ProtocolException {
        long length = in.readInt64();
        validateLength(length, MAX_TEXT_BYTES, "string data_length");
        return in.readUtf8(length, "string data_length");
    }

    private byte[] readBlob() throws IOException, ProtocolException {
        long length = in.readInt64();
        validateLength(length, MAX_BLOB_BYTES, "BLOB data_length");
        if (length > in.remaining()) {
            throw new ProtocolException("BLOB data_length exceeds remaining file bytes");
        }
        return in.readBytes((int) length);
    }

    private boolean[] decodeBitmap(byte[] bitmap, int signalCount) {
        boolean[] present = new boolean[signalCount];
        for (int i = 0; i < signalCount; i++) {
            present[i] = (bitmap[i / 8] & (1 << (i % 8))) != 0;
        }
        return present;
    }

    private void validateLength(long length, long maxBytes, String fieldName) throws ProtocolException {
        if (length < 0) {
            throw new ProtocolException(fieldName + " is larger than signed Java long can represent");
        }
        if (length > maxBytes) {
            throw new ProtocolException(fieldName + " exceeds demo safety limit: " + length);
        }
    }

    private void ensureSchema() {
        if (schema == null) {
            throw new IllegalStateException("readSchema() must be called before reading frames");
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public String toString() {
        return "BinaryRecordReader{" + "schema=" + schema + '}';
    }
}
