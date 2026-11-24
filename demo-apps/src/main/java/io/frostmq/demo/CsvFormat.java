package io.frostmq.demo;

/** RFC 4180 quoting with OWASP CSV-injection neutralization for untrusted text. */
final class CsvFormat {
    private CsvFormat() {}

    static String cell(String value) {
        String text = value == null ? "" : value;
        if (startsFormula(text)) text = "'" + text;
        return "\"" + text.replace("\"", "\"\"") + "\"";
    }

    private static boolean startsFormula(String value) {
        if (value.isEmpty()) return false;
        return switch (value.charAt(0)) {
            case '=', '+', '-', '@', '\t', '\r', '\n' -> true;
            default -> false;
        };
    }
}
