package demo.duckdb.binrecord;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

final class ColumnNameNormalizer {
    private final Set<String> used = new HashSet<>();

    String normalize(String rawName, int index) {
        String base = rawName == null ? "" : rawName.trim().toLowerCase(Locale.ROOT);
        base = base.replaceAll("[^a-z0-9_]", "_").replaceAll("_+", "_");
        base = base.replaceAll("^_+|_+$", "");
        if (base.isEmpty() || Character.isDigit(base.charAt(0))) {
            base = "signal_" + index;
        }

        String candidate = base;
        int suffix = 1;
        while (!used.add(candidate)) {
            candidate = base + "_" + suffix;
            suffix++;
        }
        return candidate;
    }

    static String quote(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }
}
