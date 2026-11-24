package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Concurrent topic-filter trie. Exact, {@code +}, and {@code #} branches are
 * walked during
 * matching, and overlapping filters are coalesced to their highest QoS.
 * Enforces per-session
 * and global membership quotas and prunes empty branches after
 * unsubscribe/session removal.
 */
public final class SubscriptionTree {
  private record State(Node node, int index) {
  }

  private static final class Node {
    final Node parent;
    final String level;
    final Map<String, Node> children = new HashMap<>();
    final Map<ClientSession, Integer> subscribers = new HashMap<>();

    Node(Node parent, String level) {
      this.parent = parent;
      this.level = level;
    }
  }

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Node root = new Node(null, null);
  private final Map<ClientSession, Set<Node>> memberships = new HashMap<>();
  private final int maxPerSession;
  private final int maxTotal;
  private int total;
  private int filterCount;

  public SubscriptionTree() {
    this(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  public SubscriptionTree(BrokerLimits limits) {
    this(limits.maxFiltersPerSession(), limits.maxFilters());
  }

  public SubscriptionTree(int maxPerSession, int maxTotal) {
    this.maxPerSession = maxPerSession;
    this.maxTotal = maxTotal;
  }

  public boolean subscribe(ClientSession session, String filter, int qos) {
    lock.writeLock().lock();
    try {
      if (session.isClosed() || !TopicMatcher.validFilter(filter))
        return false;
      String[] levels = filter.split("/", -1);
      Node node = find(levels);
      if (node != null && node.subscribers.containsKey(session)) {
        // MQTT 3.1.1 replaces the granted QoS when the same filter is subscribed again.
        // Math.max is only for distinct overlapping filters during match().
        node.subscribers.put(session, qos);
        return true;
      }
      Set<Node> owned = memberships.get(session);
      int current = owned == null ? 0 : owned.size();
      if (current >= maxPerSession || total >= maxTotal)
        return false;
      if (node == null)
        node = create(levels);
      if (node.subscribers.isEmpty())
        filterCount++;
      node.subscribers.put(session, qos);
      memberships.computeIfAbsent(session, ignored -> new HashSet<>()).add(node);
      total++;
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsubscribe(ClientSession session, String filter) {
    lock.writeLock().lock();
    try {
      if (!TopicMatcher.validFilter(filter))
        return;
      Node node = find(filter.split("/", -1));
      if (node == null || node.subscribers.remove(session) == null)
        return;
      total--;
      if (node.subscribers.isEmpty())
        filterCount--;
      Set<Node> owned = memberships.get(session);
      if (owned != null) {
        owned.remove(node);
        if (owned.isEmpty())
          memberships.remove(session);
      }
      prune(node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void remove(ClientSession session) {
    lock.writeLock().lock();
    try {
      Set<Node> owned = memberships.remove(session);
      if (owned == null)
        return;
      for (Node node : owned) {
        if (node.subscribers.remove(session) == null)
          continue;
        total--;
        if (node.subscribers.isEmpty())
          filterCount--;
        prune(node);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<ClientSession, Integer> match(String topic) {
    if (!TopicMatcher.validTopic(topic))
      return Map.of();
    String[] levels = topic.split("/", -1);
    lock.readLock().lock();
    try {
      var matched = new HashMap<ClientSession, Integer>();
      var pending = new ArrayDeque<State>();
      pending.push(new State(root, 0));
      while (!pending.isEmpty()) {
        State state = pending.pop();
        Node node = state.node();
        int index = state.index();
        if (index == levels.length) {
          merge(node, matched);
          Node hash = node.children.get("#");
          if (hash != null)
            merge(hash, matched);
          continue;
        }
        // MQTT: wildcard filters at the first level must not match $-prefixed topics.
        if (!(node == root && levels[0].startsWith("$"))) {
          Node hash = node.children.get("#");
          if (hash != null)
            merge(hash, matched);
          Node plus = node.children.get("+");
          if (plus != null)
            pending.push(new State(plus, index + 1));
        }
        Node exact = node.children.get(levels[index]);
        if (exact != null)
          pending.push(new State(exact, index + 1));
      }
      return matched;
    } finally {
      lock.readLock().unlock();
    }
  }

  public int filterCount() {
    lock.readLock().lock();
    try {
      return filterCount;
    } finally {
      lock.readLock().unlock();
    }
  }

  public int subscriptionCount() {
    lock.readLock().lock();
    try {
      return total;
    } finally {
      lock.readLock().unlock();
    }
  }

  private Node find(String[] levels) {
    Node node = root;
    for (String level : levels) {
      node = node.children.get(level);
      if (node == null)
        return null;
    }
    return node;
  }

  private Node create(String[] levels) {
    Node node = root;
    for (String level : levels) {
      Node child = node.children.get(level);
      if (child == null) {
        child = new Node(node, level);
        node.children.put(level, child);
      }
      node = child;
    }
    return node;
  }

  private static void merge(Node node, Map<ClientSession, Integer> matched) {
    node.subscribers.forEach((session, qos) -> matched.merge(session, qos, Math::max));
  }

  private void prune(Node node) {
    while (node.parent != null && node.subscribers.isEmpty() && node.children.isEmpty()) {
      Node parent = node.parent;
      parent.children.remove(node.level, node);
      node = parent;
    }
  }
}
