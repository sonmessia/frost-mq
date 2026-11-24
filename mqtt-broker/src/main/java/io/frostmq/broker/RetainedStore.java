package io.frostmq.broker;

import io.frostmq.common.PublishPacket;
import io.frostmq.common.TopicMatcher;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class RetainedStore {
    private final ConcurrentHashMap<String, PublishPacket> messages = new ConcurrentHashMap<>();
    private final int maxMessages;

    public RetainedStore() { this(Integer.MAX_VALUE); }
    public RetainedStore(int maxMessages) {
        if (maxMessages < 0) throw new IllegalArgumentException("maxMessages must be nonnegative");
        this.maxMessages = maxMessages;
    }

    /** Returns false when a new retained topic would exceed the configured quota. */
    public synchronized boolean update(PublishPacket packet) {
        if (!packet.retain()) return true;
        if (packet.payloadLength() == 0) {
            messages.remove(packet.topic());
            return true;
        }
        if (!messages.containsKey(packet.topic()) && messages.size() >= maxMessages) return false;
        messages.put(packet.topic(), new PublishPacket(packet.topic(), packet.payload(), packet.qos(), true, false, 0));
        return true;
    }

    public List<PublishPacket> matching(String filter) {
        return messages.values().stream().filter(packet -> TopicMatcher.match(filter, packet.topic())).toList();
    }
    public int size() { return messages.size(); }
}
