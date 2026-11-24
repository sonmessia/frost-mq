package io.frostmq.common;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public final class PacketCodec {
  public static final int MAX_PACKET_SIZE = 262_144;

  private PacketCodec() {
  }

  public static byte[] encodeRemainingLength(int value) throws IOException {
    if (value < 0 || value > MAX_PACKET_SIZE)
      throw new IOException("Packet exceeds 256 KB limit");
    var out = new ByteArrayOutputStream();
    do {
      int digit = value % 128;
      value /= 128;
      out.write(value > 0 ? digit | 128 : digit);
    } while (value > 0);
    return out.toByteArray();
  }

  public static int decodeRemainingLength(InputStream in) throws IOException {
    int value = 0;
    int multiplier = 1;
    for (int count = 1; count <= 4; count++) {
      int digit = in.read();
      if (digit < 0)
        throw new EOFException("Truncated Remaining Length");
      value += (digit & 127) * multiplier;
      if ((digit & 128) == 0) {
        if (count > 1 && digit == 0)
          throw new IOException("Noncanonical Remaining Length");
        if (value > MAX_PACKET_SIZE)
          throw new IOException("Packet exceeds 256 KB limit");
        return value;
      }
      multiplier *= 128;
    }
    throw new IOException("Remaining Length exceeds four bytes");
  }

  public static void readFully(InputStream in, byte[] buffer) throws IOException {
    int position = 0;
    while (position < buffer.length) {
      int n = in.read(buffer, position, buffer.length - position);
      if (n < 0)
        throw new EOFException("Truncated MQTT packet");
      if (n == 0) {
        int single = in.read();
        if (single < 0)
          throw new EOFException("Truncated MQTT packet");
        buffer[position++] = (byte) single;
      } else
        position += n;
    }
  }

  /** Returns null only for EOF before a packet header. */
  public static MqttPacket decode(InputStream source) throws IOException {
    int header = source.read();
    if (header < 0)
      return null;
    MqttPacketType type = MqttPacketType.fromHeader(header);
    int flags = header & 15;
    if (type != MqttPacketType.PUBLISH) {
      int expected = type == MqttPacketType.SUBSCRIBE || type == MqttPacketType.UNSUBSCRIBE ? 2 : 0;
      require(flags == expected, "Invalid fixed-header flags");
    }
    byte[] body = new byte[decodeRemainingLength(source)];
    readFully(source, body);
    var in = new DataInputStream(new ByteArrayInputStream(body));
    MqttPacket packet = switch (type) {
      case CONNECT -> readConnect(in);
      case CONNACK -> {
        int present = in.readUnsignedByte();
        int code = in.readUnsignedByte();
        require(present <= 1 && code <= 5 && (code == 0 || present == 0), "Invalid CONNACK");
        yield new ConnackPacket(present == 1, code);
      }
      case PUBLISH -> {
        int qos = (flags >>> 1) & 3;
        require(qos != 3 && !(qos == 0 && (flags & 8) != 0), "Invalid PUBLISH flags");
        String topic = readString(in);
        require(TopicMatcher.validTopic(topic), "Invalid PUBLISH topic");
        int id = qos > 0 ? readId(in) : 0;
        yield new PublishPacket(topic, in.readAllBytes(), qos, (flags & 1) != 0, (flags & 8) != 0, id);
      }
      case PUBACK -> new PubackPacket(readId(in));
      case SUBSCRIBE -> {
        int id = readId(in);
        var filters = new ArrayList<SubscribePacket.Subscription>();
        while (in.available() > 0) {
          String filter = readString(in);
          int qos = in.readUnsignedByte();
          require(TopicMatcher.validFilter(filter) && qos <= 2, "Invalid subscription");
          filters.add(new SubscribePacket.Subscription(filter, qos));
        }
        require(!filters.isEmpty(), "Empty SUBSCRIBE");
        yield new SubscribePacket(id, filters);
      }
      case SUBACK -> {
        int id = readId(in);
        var codes = new ArrayList<Integer>();
        while (in.available() > 0) {
          int code = in.readUnsignedByte();
          require(code == 0 || code == 1 || code == 2 || code == 128, "Invalid SUBACK code");
          codes.add(code);
        }
        require(!codes.isEmpty(), "Empty SUBACK");
        yield new SubackPacket(id, codes);
      }
      case UNSUBSCRIBE -> {
        int id = readId(in);
        var filters = new ArrayList<String>();
        while (in.available() > 0) {
          String filter = readString(in);
          require(TopicMatcher.validFilter(filter), "Invalid unsubscribe filter");
          filters.add(filter);
        }
        require(!filters.isEmpty(), "Empty UNSUBSCRIBE");
        yield new UnsubscribePacket(id, filters);
      }
      case UNSUBACK -> new UnsubackPacket(readId(in));
      case PINGREQ -> new PingReqPacket();
      case PINGRESP -> new PingRespPacket();
      case DISCONNECT -> new DisconnectPacket();
    };
    require(in.available() == 0, "Unexpected trailing packet bytes");
    return packet;
  }

  private static ConnectPacket readConnect(DataInputStream in) throws IOException {
    String protocol = readString(in);
    int level = in.readUnsignedByte();
    int flags = in.readUnsignedByte();
    require((flags & 1) == 0, "Reserved CONNECT flag");
    boolean hasWill = (flags & 4) != 0;
    int willQos = (flags >>> 3) & 3;
    boolean willRetain = (flags & 32) != 0;
    require(willQos != 3 && (hasWill || (willQos == 0 && !willRetain)), "Invalid Will flags");
    require((flags & 64) == 0 || (flags & 128) != 0, "Password without username");
    int keepAlive = in.readUnsignedShort();
    String id = readString(in);
    PublishPacket will = null;
    if (hasWill) {
      String topic = readString(in);
      require(TopicMatcher.validTopic(topic), "Invalid Will topic");
      will = new PublishPacket(topic, readBinary(in), willQos, willRetain, false, 0);
    }
    String username = (flags & 128) != 0 ? readString(in) : null;
    byte[] password = (flags & 64) != 0 ? readBinary(in) : null;
    return new ConnectPacket(protocol, level, id, (flags & 2) != 0, keepAlive, username, password, will);
  }

  public static void encode(MqttPacket packet, OutputStream target) throws IOException {
    var bytes = new ByteArrayOutputStream();
    var out = new DataOutputStream(bytes);
    int flags = 0;
    switch (packet) {
      case ConnectPacket p -> {
        writeString(out, p.protocolName());
        require(p.protocolLevel() >= 0 && p.protocolLevel() <= 255, "Invalid protocol level");
        out.writeByte(p.protocolLevel());
        int connectFlags = p.cleanSession() ? 2 : 0;
        PublishPacket will = p.willMessage();
        if (will != null) {
          require(will.qos() >= 0 && will.qos() <= 2 && TopicMatcher.validTopic(will.topic()), "Invalid Will");
          connectFlags |= 4 | (will.qos() << 3) | (will.retain() ? 32 : 0);
        }
        if (p.username() != null)
          connectFlags |= 128;
        if (p.password() != null) {
          require(p.username() != null, "Password without username");
          connectFlags |= 64;
        }
        out.writeByte(connectFlags);
        writeShort(out, p.keepAliveSeconds());
        writeString(out, p.clientId());
        if (will != null) {
          writeString(out, will.topic());
          writeBinary(out, will.payload());
        }
        if (p.username() != null)
          writeString(out, p.username());
        if (p.password() != null)
          writeBinary(out, p.password());
      }
      case ConnackPacket p -> {
        require(p.returnCode() >= 0 && p.returnCode() <= 5 && (!p.sessionPresent() || p.returnCode() == 0),
            "Invalid CONNACK");
        out.writeByte(p.sessionPresent() ? 1 : 0);
        out.writeByte(p.returnCode());
      }
      case PublishPacket p -> {
        require(p.qos() >= 0 && p.qos() <= 2 && !(p.qos() == 0 && p.dup()), "Invalid QoS");
        require(TopicMatcher.validTopic(p.topic()), "Invalid PUBLISH topic");
        flags = (p.qos() << 1) | (p.retain() ? 1 : 0) | (p.dup() ? 8 : 0);
        writeString(out, p.topic());
        if (p.qos() > 0)
          writeId(out, p.packetId());
        out.write(p.payload());
      }
      case PubackPacket p -> writeId(out, p.packetId());
      case SubscribePacket p -> {
        flags = 2;
        writeId(out, p.packetId());
        require(!p.subscriptions().isEmpty(), "Empty SUBSCRIBE");
        for (var subscription : p.subscriptions()) {
          require(TopicMatcher.validFilter(subscription.filter()) && subscription.qos() >= 0 && subscription.qos() <= 2,
              "Invalid subscription");
          writeString(out, subscription.filter());
          out.writeByte(subscription.qos());
        }
      }
      case SubackPacket p -> {
        writeId(out, p.packetId());
        require(!p.returnCodes().isEmpty(), "Empty SUBACK");
        for (int code : p.returnCodes()) {
          require(code == 0 || code == 1 || code == 2 || code == 128, "Invalid SUBACK code");
          out.writeByte(code);
        }
      }
      case UnsubscribePacket p -> {
        flags = 2;
        writeId(out, p.packetId());
        require(!p.filters().isEmpty(), "Empty UNSUBSCRIBE");
        for (String filter : p.filters()) {
          require(TopicMatcher.validFilter(filter), "Invalid unsubscribe filter");
          writeString(out, filter);
        }
      }
      case UnsubackPacket p -> writeId(out, p.packetId());
      case PingReqPacket ignored -> {
      }
      case PingRespPacket ignored -> {
      }
      case DisconnectPacket ignored -> {
      }
    }
    byte[] body = bytes.toByteArray();
    byte[] length = encodeRemainingLength(body.length);
    target.write((packet.type().code() << 4) | flags);
    target.write(length);
    target.write(body);
    target.flush();
  }

  private static int readId(DataInputStream in) throws IOException {
    int id = in.readUnsignedShort();
    require(id != 0, "Zero packet identifier");
    return id;
  }

  private static void writeId(DataOutputStream out, int id) throws IOException {
    require(id > 0 && id <= 65535, "Invalid packet identifier");
    out.writeShort(id);
  }

  private static void writeShort(DataOutputStream out, int value) throws IOException {
    require(value >= 0 && value <= 65535, "Invalid unsigned short");
    out.writeShort(value);
  }

  private static byte[] readBinary(DataInputStream in) throws IOException {
    byte[] data = new byte[in.readUnsignedShort()];
    readFully(in, data);
    return data;
  }

  private static void writeBinary(DataOutputStream out, byte[] data) throws IOException {
    writeShort(out, data.length);
    out.write(data);
  }

  private static String readString(DataInputStream in) throws IOException {
    try {
      String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(readBinary(in))).toString();
      require(value.indexOf('\0') < 0, "NUL in MQTT UTF-8 string");
      return value;
    } catch (CharacterCodingException e) {
      throw new IOException("Invalid MQTT UTF-8", e);
    }
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    require(value != null && value.indexOf('\0') < 0, "Invalid MQTT string");
    try {
      ByteBuffer data = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      writeBinary(out, bytes);
    } catch (CharacterCodingException e) {
      throw new IOException("Invalid MQTT UTF-8", e);
    }
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition)
      throw new IOException(message);
  }
}
