package demo.duckdb.binrecord;

import java.util.HashMap;
import java.util.Map;

enum SignalType {
    UINT64(0, 8, "kUint64"),
    UINT32(1, 4, "kUint32"),
    UINT16(2, 2, "kUint16"),
    UINT8(3, 1, "kUint8"),
    INT64(4, 8, "kInt64"),
    INT32(5, 4, "kInt32"),
    INT16(6, 2, "kInt16"),
    INT8(7, 1, "kInt8"),
    BOOL(8, 1, "kBool"),
    ENUM(9, 8, "kEnum"),
    FLOAT(10, 4, "kFloat"),
    DOUBLE(11, 8, "kDouble"),
    STRING(12, -1, "kString"),
    BINARY(13, -1, "kBinary"),
    COMPLEX32(14, 8, "kComplex32"),
    COMPLEX64(15, 16, "kComplex64");

    private static final Map<Integer, SignalType> BY_CODE = new HashMap<>();

    static {
        for (SignalType type : values()) {
            BY_CODE.put(type.code, type);
        }
    }

    final int code;
    final int fixedSize;
    final String protocolName;

    SignalType(int code, int fixedSize, String protocolName) {
        this.code = code;
        this.fixedSize = fixedSize;
        this.protocolName = protocolName;
    }

    static SignalType fromCode(int code) throws ProtocolException {
        SignalType type = BY_CODE.get(code);
        if (type == null) {
            throw new ProtocolException("Unsupported signal type code: " + code);
        }
        return type;
    }

    boolean isVariableLength() {
        return this == STRING || this == BINARY;
    }

    boolean isArraySupportedByCurrentProtocol() {
        return !isVariableLength();
    }
}
