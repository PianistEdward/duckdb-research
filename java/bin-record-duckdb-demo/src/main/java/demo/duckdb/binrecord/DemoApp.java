package demo.duckdb.binrecord;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

public final class DemoApp {
    private DemoApp() {
    }

    public static void main(String[] args) throws Exception {
        if (containsHelp(args)) {
            Options.printHelp();
            return;
        }
        Options options = Options.parse(args);
        if (options.generateSample()) {
            SampleBinaryRecordWriter.writeSample(options.binPath());
            System.out.println("Generated sample binary: " + options.binPath().toAbsolutePath());
        }

        DuckDbImporter importer = new DuckDbImporter();
        BinaryRecordImportResult result = importer.importFile(options.binPath(), options.databasePath(), options.tableName());
        System.out.println("Imported frames: " + result.frameCount());
        System.out.println("Signals: " + result.schema().signalCount());
        System.out.println("DuckDB database: " + options.databasePath().toAbsolutePath());
        System.out.println("DuckDB table: " + options.tableName());

        // Demo query proves the appender wrote sparse frames as NULL-able DuckDB rows.
        List<String> rows = importer.previewRows(options.databasePath(), options.tableName(), 10);
        for (String row : rows) {
            System.out.println(row);
        }
    }

    record Options(Path binPath, Path databasePath, String tableName, boolean generateSample) {
        static Options parse(String[] args) throws SQLException {
            Path bin = Path.of("target/demo-data/sim.bin");
            Path db = Path.of("target/demo-data/demo.duckdb");
            String table = "sim_record";
            boolean generate = true;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--bin" -> bin = Path.of(requireValue(args, ++i, "--bin"));
                    case "--db" -> db = Path.of(requireValue(args, ++i, "--db"));
                    case "--table" -> table = requireValue(args, ++i, "--table");
                    case "--no-generate" -> generate = false;
                    default -> throw new SQLException("Unknown argument: " + args[i]);
                }
            }
            return new Options(bin, db, table, generate);
        }

        private static String requireValue(String[] args, int index, String option) throws SQLException {
            if (index >= args.length) {
                throw new SQLException(option + " requires a value");
            }
            return args[index];
        }

        static void printHelp() {
            System.out.println("""
                    Usage:
                      mvn -q package
                      java -cp "target/classes:../../source_code/libs/duckdb_jdbc-1.5.2.1.jar" demo.duckdb.binrecord.DemoApp

                    Options:
                      --bin <path>       Binary record file path. Default: target/demo-data/sim.bin
                      --db <path>        DuckDB database path. Default: target/demo-data/demo.duckdb
                      --table <name>     Output table. Default: sim_record
                      --no-generate      Import existing --bin instead of writing sample data first
                    """);
        }
    }

    private static boolean containsHelp(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg)) {
                return true;
            }
        }
        return false;
    }
}
