package demo.duckdb.binrecord;

/**
 * Checked failures for protocol validation. A malformed file should fail before
 * partially imported rows are treated as valid data.
 */
public class ProtocolException extends Exception {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
