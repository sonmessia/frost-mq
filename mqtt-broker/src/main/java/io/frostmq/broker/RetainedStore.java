package io.frostmq.broker;

import io.frostmq.common.PublishPacket;
import io.frostmq.common.TopicMatcher;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class RetainedStore {
    private final ConcurrentHashMap<String, PublishPacket> messages = new ConcurrentHashMap<>();

    public void update(PublishPacket packet) {
        if (!packet.retain()) return;
        if (packet.payloadLength() == 0) messages.remove(packet.topic());
        else messages.put(packet.topic(), new PublishPacket(packet.topic(), packet.payload(), packet.qos(), true, false, 0));
    }

    public List<PublishPacket> matching(String filter) {
        return messages.values().stream().filter(packet -> TopicMatcher.match(filter, packet.topic())).toList();
    }
    public int size() { return messages.size(); }
}
