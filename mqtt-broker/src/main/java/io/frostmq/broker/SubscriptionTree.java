package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Locked concurrent filter index. Matching coalesces overlapping filters to their highest QoS. */
public final class SubscriptionTree {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Map<ClientSession, Integer>> filters = new HashMap<>();

    public void subscribe(ClientSession session, String filter, int qos) {
        lock.writeLock().lock();
        try {
            if (!session.isClosed()) filters.computeIfAbsent(filter, ignored -> new HashMap<>()).put(session, qos);
        } finally { lock.writeLock().unlock(); }
    }

    public void unsubscribe(ClientSession session, String filter) {
        lock.writeLock().lock();
        try {
            Map<ClientSession, Integer> members = filters.get(filter);
            if (members != null) {
                members.remove(session);
                if (members.isEmpty()) filters.remove(filter);
            }
        } finally { lock.writeLock().unlock(); }
    }

    public void remove(ClientSession session) {
        lock.writeLock().lock();
        try {
            filters.values().forEach(members -> members.remove(session));
            filters.entrySet().removeIf(entry -> entry.getValue().isEmpty());
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
}
