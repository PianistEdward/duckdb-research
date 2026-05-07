package demo.duckdb.binrecord;

import java.util.List;

record BinaryRecordSchema(long version, List<SignalMeta> signals) {
    BinaryRecordSchema {
        signals = List.copyOf(signals);
    }

    int signalCount() {
        return signals.size();
    }

    SignalMeta signal(int index) {
        return signals.get(index);
    }
}
