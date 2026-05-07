package demo.duckdb.binrecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

final class DuckDbImporter {
    BinaryRecordImportResult importFile(Path binPath, Path databasePath, String tableName)
            throws IOException, ProtocolException, SQLException {
        Path databaseParent = databasePath.toAbsolutePath().getParent();
        if (databaseParent != null) {
            Files.createDirectories(databaseParent);
        }
        try (BinaryRecordReader reader = new BinaryRecordReader(binPath)) {
            BinaryRecordSchema schema = reader.readSchema();
            ImportSchema importSchema = ImportSchema.from(schema, tableName);

            try (Connection raw = DriverManager.getConnection("jdbc:duckdb:" + databasePath.toAbsolutePath())) {
                raw.setAutoCommit(false);
                try (Statement statement = raw.createStatement()) {
                    DuckDBConnection duck = raw.unwrap(DuckDBConnection.class);
                    recreateTables(statement, importSchema);
                    writeSignalMetadata(raw, importSchema);

                    long frameCount = appendFrames(reader, duck, importSchema);
                    writeFileMetadata(raw, binPath, importSchema, schema, frameCount);
                    raw.commit();
                    return new BinaryRecordImportResult(schema, frameCount, true);
                } catch (IOException | ProtocolException | SQLException | RuntimeException error) {
                    rollbackQuietly(raw);
                    throw error;
                }
            }
        }
    }

    List<String> previewRows(Path databasePath, String tableName, int limit) throws SQLException {
        List<String> rows = new ArrayList<>();
        String sql = "SELECT * FROM "
                + ColumnNameNormalizer.quote(tableName)
                + " LIMIT " + limit;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:" + databasePath.toAbsolutePath());
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if (i > 1) {
                        row.append(", ");
                    }
                    row.append(meta.getColumnName(i)).append('=').append(rs.getString(i));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    private long appendFrames(BinaryRecordReader reader, DuckDBConnection duck, ImportSchema importSchema)
            throws IOException, ProtocolException, SQLException {
        long frameCount = 0;
        try (DuckDBAppender appender = duck.createAppender(importSchema.tableName())) {
            BinaryRecordFrame frame;
            while ((frame = reader.readNextFrameOrNull()) != null) {
                appender.beginRow();
                for (ImportColumn column : importSchema.columns()) {
                    int signalIndex = column.signal().index();
                    if (!frame.hasValue(signalIndex)) {
                        appender.appendNull();
                    } else {
                        DuckDbTypeMapper.appendValue(appender, column.signal(), frame.value(signalIndex));
                    }
                }
                appender.endRow();
                frameCount++;
            }
            appender.flush();
        }
        return frameCount;
    }

    private void recreateTables(Statement statement, ImportSchema schema) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS " + ColumnNameNormalizer.quote(schema.tableName()));
        statement.execute("CREATE TABLE " + ColumnNameNormalizer.quote(schema.tableName()) + " ("
                + schema.createColumnSql() + ")");

        statement.execute("CREATE TABLE IF NOT EXISTS bin_record_signal_meta ("
                + "import_table VARCHAR, "
                + "signal_index UBIGINT, "
                + "original_name VARCHAR, "
                + "column_name VARCHAR, "
                + "signal_type UTINYINT, "
                + "dimension_count UINTEGER, "
                + "dimensions_json VARCHAR, "
                + "formula VARCHAR, "
                + "duckdb_type VARCHAR)");

        statement.execute("CREATE TABLE IF NOT EXISTS bin_record_file_meta ("
                + "import_table VARCHAR, "
                + "file_path VARCHAR, "
                + "format_version UBIGINT, "
                + "signal_count UBIGINT, "
                + "imported_frame_count UBIGINT, "
                + "eof_valid BOOLEAN, "
                + "file_size_bytes UBIGINT, "
                + "imported_at TIMESTAMP)");

        try (PreparedStatement deleteSignals = statement.getConnection()
                .prepareStatement("DELETE FROM bin_record_signal_meta WHERE import_table = ?");
             PreparedStatement deleteFiles = statement.getConnection()
                     .prepareStatement("DELETE FROM bin_record_file_meta WHERE import_table = ?")) {
            deleteSignals.setString(1, schema.tableName());
            deleteSignals.executeUpdate();
            deleteFiles.setString(1, schema.tableName());
            deleteFiles.executeUpdate();
        }
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // Preserve the original import failure. Connection close still releases native resources.
        }
    }

    private void writeSignalMetadata(Connection connection, ImportSchema schema) throws SQLException {
        String sql = "INSERT INTO bin_record_signal_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ImportColumn column : schema.columns()) {
                SignalMeta signal = column.signal();
                ps.setString(1, schema.tableName());
                ps.setLong(2, signal.index());
                ps.setString(3, signal.name());
                ps.setString(4, column.columnName());
                ps.setInt(5, signal.type().code);
                ps.setInt(6, signal.dimensions().length);
                ps.setString(7, signal.dimensionsJson());
                ps.setString(8, signal.formula());
                ps.setString(9, column.duckDbType());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeFileMetadata(Connection connection, Path binPath, ImportSchema importSchema,
            BinaryRecordSchema schema, long frameCount)
            throws SQLException, IOException {
        String sql = "INSERT INTO bin_record_file_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, importSchema.tableName());
            ps.setString(2, binPath.toAbsolutePath().toString());
            ps.setLong(3, schema.version());
            ps.setLong(4, schema.signalCount());
            ps.setLong(5, frameCount);
            ps.setBoolean(6, true);
            ps.setLong(7, Files.size(binPath));
            ps.setTimestamp(8, Timestamp.from(Instant.now()));
            ps.executeUpdate();
        }
    }

    record ImportColumn(SignalMeta signal, String columnName, String duckDbType) {
        String createSql() {
            return ColumnNameNormalizer.quote(columnName) + " " + duckDbType;
        }
    }

    record ImportSchema(String tableName, List<ImportColumn> columns) {
        static ImportSchema from(BinaryRecordSchema schema, String tableName) {
            validateTableName(tableName);
            ColumnNameNormalizer normalizer = new ColumnNameNormalizer();
            List<ImportColumn> columns = new ArrayList<>();
            for (SignalMeta signal : schema.signals()) {
                String columnName = normalizer.normalize(signal.name(), signal.index());
                columns.add(new ImportColumn(signal, columnName, DuckDbTypeMapper.duckDbType(signal)));
            }
            return new ImportSchema(tableName, List.copyOf(columns));
        }

        String createColumnSql() {
            return columns.stream()
                    .map(ImportColumn::createSql)
                    .collect(java.util.stream.Collectors.joining(", "));
        }

        private static void validateTableName(String tableName) {
            if (tableName == null || !tableName.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("Table name must match [A-Za-z_][A-Za-z0-9_]*");
            }
        }
    }
}
