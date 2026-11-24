package io.frostmq.demo;

import io.frostmq.common.TopicMatcher;
import java.util.*;

record DemoSettings(String host, int port, String site, String password, Map<String, String> flags) {
  static DemoSettings parse(String[] args, Set<String> extraFlags) {
    var allowed = new HashSet<>(Set.of("host", "port", "site"));
    allowed.addAll(extraFlags);
    var flags = new HashMap<String, String>();
    for (int i = 0; i < args.length; i += 2) {
      if (!args[i].startsWith("--") || i + 1 >= args.length || !allowed.contains(args[i].substring(2)))
        throw new IllegalArgumentException("Expected --name value; supported: " + allowed);
      flags.put(args[i].substring(2), args[i + 1]);
    }
    int port = Integer.parseInt(flags.getOrDefault("port", "1883"));
    String site = flags.getOrDefault("site", "warehouse-a");
    if (port < 1 || port > 65535 || !TopicMatcher.validTopic(site) || site.contains("/"))
      throw new IllegalArgumentException("Invalid port or site");
    return new DemoSettings(flags.getOrDefault("host", "127.0.0.1"), port, site,
        System.getenv().getOrDefault("FROSTMQ_PASSWORD", "frostmq-demo"), Map.copyOf(flags));
  }

  int number(String name, int fallback) {
    return Integer.parseInt(flags.getOrDefault(name, Integer.toString(fallback)));
  }

  String text(String name, String fallback) {
    return flags.getOrDefault(name, fallback);
  }
}
