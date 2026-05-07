package demo.duckdb.binrecord;

record ComplexValue(double re, double im) {
    double[] asArray() {
        return new double[] {re, im};
    }
}
