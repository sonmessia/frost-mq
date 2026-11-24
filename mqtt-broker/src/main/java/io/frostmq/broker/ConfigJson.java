package io.frostmq.broker;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/** Small strict JSON reader for configuration, keeping the broker JDK-only. */
final class ConfigJson {
  private final String source;
  private int position;

  private ConfigJson(String source) {
    this.source = source;
  }

  static Object parse(String source) throws IOException {
    var parser = new ConfigJson(source);
    Object value = parser.value(0);
    parser.space();
    if (parser.position != source.length())
      throw new IOException("Trailing JSON content");
    return value;
  }

  private Object value(int depth) throws IOException {
    if (depth > 32)
      throw error("JSON nesting limit exceeded");
    space();
    if (position >= source.length())
      throw error("Unexpected end of JSON");
    char c = source.charAt(position);
    if (c == '"')
      return string();
    if (c == '{') {
      position++;
      var map = new LinkedHashMap<String, Object>();
      space();
      if (take('}'))
        return map;
      do {
        space();
        String key = string();
        space();
        expect(':');
        Object item = value(depth + 1);
        if (map.containsKey(key))
          throw error("Duplicate JSON key");
        map.put(key, item);
        space();
        if (take('}'))
          return map;
        expect(',');
      } while (true);
    }
    if (c == '[') {
      position++;
      var list = new ArrayList<Object>();
      space();
      if (take(']'))
        return list;
      do {
        list.add(value(depth + 1));
        space();
        if (take(']'))
          return list;
        expect(',');
      } while (true);
    }
    for (String literal : List.of("true", "false", "null")) {
      if (source.startsWith(literal, position)) {
        position += literal.length();
        return literal.equals("null") ? null : literal.equals("true");
      }
    }
    int start = position;
    while (position < source.length() && "-+0123456789.eE".indexOf(source.charAt(position)) >= 0)
      position++;
    String number = source.substring(start, position);
    if (!number.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))
      throw error("Invalid JSON value");
    try {
      return new BigDecimal(number);
    } catch (NumberFormatException e) {
      throw error("Invalid JSON number");
    }
  }

  private String string() throws IOException {
    expect('"');
    var result = new StringBuilder();
    while (position < source.length()) {
      char c = source.charAt(position++);
      if (c == '"') {
        String text = result.toString();
        for (int i = 0; i < text.length(); i++) {
          if (Character.isHighSurrogate(text.charAt(i))) {
            if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
              throw error("Unpaired surrogate");
          } else if (Character.isLowSurrogate(text.charAt(i)))
            throw error("Unpaired surrogate");
        }
        return text;
      }
      if (c < 32)
        throw error("Control character in JSON string");
      if (c == '\\') {
        if (position >= source.length())
          throw error("Truncated escape");
        c = source.charAt(position++);
        switch (c) {
          case '"', '\\', '/' -> result.append(c);
          case 'b' -> result.append('\b');
          case 'f' -> result.append('\f');
          case 'n' -> result.append('\n');
          case 'r' -> result.append('\r');
          case 't' -> result.append('\t');
          case 'u' -> {
            if (position + 4 > source.length())
              throw error("Truncated Unicode escape");
            try {
              result.append((char) Integer.parseInt(source.substring(position, position + 4), 16));
            } catch (NumberFormatException e) {
              throw error("Invalid Unicode escape");
            }
            position += 4;
          }
          default -> throw error("Invalid string escape");
        }
      } else
        result.append(c);
    }
    throw error("Unterminated string");
  }

  private void space() {
    while (position < source.length() && " \r\n\t".indexOf(source.charAt(position)) >= 0)
      position++;
  }

  private boolean take(char c) {
    if (position < source.length() && source.charAt(position) == c) {
      position++;
      return true;
    }
    return false;
  }

  private void expect(char c) throws IOException {
    if (!take(c))
      throw error("Expected '" + c + "'");
  }

  private IOException error(String message) {
    return new IOException(message + " at offset " + position);
  }
}
