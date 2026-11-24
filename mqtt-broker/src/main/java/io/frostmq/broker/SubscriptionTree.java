package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Locked concurrent filter index with per-session and total membership quotas. */
public final class SubscriptionTree {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Map<ClientSession, Integer>> filters = new HashMap<>();
    private final Map<ClientSession, Integer> counts = new HashMap<>();
    private final int maxPerSession;
    private final int maxTotal;
    private int total;

    public SubscriptionTree() { this(Integer.MAX_VALUE, Integer.MAX_VALUE); }
    public SubscriptionTree(BrokerLimits limits) { this(limits.maxFiltersPerSession(), limits.maxFilters()); }
    public SubscriptionTree(int maxPerSession, int maxTotal) {
        this.maxPerSession = maxPerSession;
        this.maxTotal = maxTotal;
    }

    public boolean subscribe(ClientSession session, String filter, int qos) {
        lock.writeLock().lock();
        try {
            if (session.isClosed()) return false;
            Map<ClientSession, Integer> members = filters.get(filter);
            if (members != null && members.containsKey(session)) {
                members.put(session, Math.max(members.get(session), qos));
                return true;
            }
            int current = counts.getOrDefault(session, 0);
            if (current >= maxPerSession || total >= maxTotal) return false;
            filters.computeIfAbsent(filter, ignored -> new HashMap<>()).put(session, qos);
            counts.put(session, current + 1);
            total++;
            return true;
        } finally { lock.writeLock().unlock(); }
    }

    public void unsubscribe(ClientSession session, String filter) {
        lock.writeLock().lock();
        try {
            Map<ClientSession, Integer> members = filters.get(filter);
            if (members == null) return;
            if (members.remove(session) != null) removed(session);
            if (members.isEmpty()) filters.remove(filter);
        } finally { lock.writeLock().unlock(); }
    }

    public void remove(ClientSession session) {
        lock.writeLock().lock();
        try {
            int removed = 0;
            for (var iterator = filters.entrySet().iterator(); iterator.hasNext();) {
                var entry = iterator.next();
                if (entry.getValue().remove(session) != null) removed++;
                if (entry.getValue().isEmpty()) iterator.remove();
            }
            if (removed > 0) {
                total -= removed;
                counts.remove(session);
            }
        } finally { lock.writeLock().unlock(); }
    }

    public Map<ClientSession, Integer> match(String topic) {
        lock.readLock().lock();
        try {
            var matched = new HashMap<ClientSession, Integer>();
            filters.forEach((filter, members) -> {
                if (TopicMatcher.match(filter, topic)) members.forEach((session, qos) -> matched.merge(session, qos, Math::max));
            });
            return matched;
        } finally { lock.readLock().unlock(); }
    }

    public int filterCount() {
        lock.readLock().lock();
        try { return filters.size(); }
        finally { lock.readLock().unlock(); }
    }

    public int subscriptionCount() {
        lock.readLock().lock();
        try { return total; }
        finally { lock.readLock().unlock(); }
    }

    private void removed(ClientSession session) {
        total--;
        counts.computeIfPresent(session, (ignored, count) -> count <= 1 ? null : count - 1);
    }
}
