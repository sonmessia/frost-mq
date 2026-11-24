package io.frostmq.broker;

/**
 * Resource ceilings that keep an in-memory broker from growing without bound.
 */
public record BrokerLimits(int maxConnections, int maxFiltersPerSession, int maxFilters, int maxRetainedMessages) {
  public static final BrokerLimits DEFAULT = new BrokerLimits(1_024, 100, 10_000, 10_000);

  public BrokerLimits {
    if (maxConnections < 1)
      throw new IllegalArgumentException("maxConnections must be positive");
    if (maxFiltersPerSession < 0 || maxFilters < 0 || maxRetainedMessages < 0)
      throw new IllegalArgumentException("Subscription and retained limits must be nonnegative");
  }

  public static BrokerLimits fromEnvironment() {
    return new BrokerLimits(
        number("FROSTMQ_MAX_CONNECTIONS", DEFAULT.maxConnections()),
        number("FROSTMQ_MAX_FILTERS_PER_SESSION", DEFAULT.maxFiltersPerSession()),
        number("FROSTMQ_MAX_FILTERS", DEFAULT.maxFilters()),
        number("FROSTMQ_MAX_RETAINED", DEFAULT.maxRetainedMessages()));
  }

  private static int number(String name, int fallback) {
    String value = System.getenv(name);
    if (value == null || value.isBlank())
      return fallback;
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be an integer", e);
    }
  }
}
