package demo.duckdb.binrecord;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

final class LittleEndianData {
    private final SeekableByteChannel channel;
    private final ByteBuffer scratch = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);

    LittleEndianData(SeekableByteChannel channel) {
        this.channel = channel;
    }

    long position() throws IOException {
        return channel.position();
    }

    long size() throws IOException {
        return channel.size();
    }

    void position(long position) throws IOException {
        channel.position(position);
    }

    int readUnsignedByte() throws IOException {
        scratch.clear().limit(1);
        readFully(scratch);
        scratch.flip();
        return Byte.toUnsignedInt(scratch.get());
    }

    short readInt8() throws IOException {
        scratch.clear().limit(1);
        readFully(scratch);
        scratch.flip();
        return scratch.get();
    }

    int readUInt16AsInt() throws IOException {
        scratch.clear().limit(Short.BYTES);
        readFully(scratch);
        scratch.flip();
        return Short.toUnsignedInt(scratch.getShort());
    }

    short readInt16() throws IOException {
        scratch.clear().limit(Short.BYTES);
        readFully(scratch);
        scratch.flip();
        return scratch.getShort();
    }

    long readUInt32AsLong() throws IOException {
        scratch.clear().limit(Integer.BYTES);
        readFully(scratch);
        scratch.flip();
        return Integer.toUnsignedLong(scratch.getInt());
    }

    int readInt32() throws IOException {
        scratch.clear().limit(Integer.BYTES);
        readFully(scratch);
        scratch.flip();
        return scratch.getInt();
    }

    long readInt64() throws IOException {
        scratch.clear().limit(Long.BYTES);
        readFully(scratch);
        scratch.flip();
        return scratch.getLong();
    }

    float readFloat() throws IOException {
        scratch.clear().limit(Float.BYTES);
        readFully(scratch);
        scratch.flip();
        return scratch.getFloat();
    }

    double readDouble() throws IOException {
        scratch.clear().limit(Double.BYTES);
        readFully(scratch);
        scratch.flip();
        return scratch.getDouble();
    }

    byte[] readBytes(int length) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        readFully(buffer);
        return buffer.array();
    }

    String readUtf8(long byteLength, String fieldName) throws IOException, ProtocolException {
        if (byteLength < 0) {
            throw new ProtocolException(fieldName + " is larger than signed Java long can represent");
        }
        if (byteLength > Integer.MAX_VALUE) {
            throw new ProtocolException(fieldName + " is too large for demo reader: " + byteLength);
        }
        if (byteLength > remaining()) {
            throw new ProtocolException(fieldName + " exceeds remaining file bytes");
        }
        return new String(readBytes((int) byteLength), StandardCharsets.UTF_8);
    }

    long remaining() throws IOException {
        return size() - position();
    }

    private void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new EOFException("Unexpected end of file at offset " + channel.position());
            }
        }
    }
}
