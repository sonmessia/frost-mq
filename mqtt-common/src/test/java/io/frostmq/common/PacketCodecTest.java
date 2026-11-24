package io.frostmq.common;

import org.junit.jupiter.api.Test;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PacketCodecTest {
  @Test
  void remainingLengthBoundaries() throws Exception {
    int[] values = { 0, 127, 128, 16383, 16384, 262144 };
    int[] sizes = { 1, 1, 2, 2, 3, 3 };
    for (int i = 0; i < values.length; i++) {
      byte[] encoded = PacketCodec.encodeRemainingLength(values[i]);
      assertEquals(sizes[i], encoded.length);
      assertEquals(values[i], PacketCodec.decodeRemainingLength(new ByteArrayInputStream(encoded)));
    }
    assertArrayEquals(new byte[] { (byte) 0x80, 1 }, PacketCodec.encodeRemainingLength(128));
    assertArrayEquals(new byte[] { (byte) 0xff, 0x7f }, PacketCodec.encodeRemainingLength(16383));
    assertArrayEquals(new byte[] { (byte) 0x80, (byte) 0x80, 1 }, PacketCodec.encodeRemainingLength(16384));
    assertThrows(IOException.class, () -> PacketCodec.encodeRemainingLength(-1));
    assertThrows(IOException.class, () -> PacketCodec.encodeRemainingLength(262145));
  }

  @Test
  void malformedRemainingLength() {
    for (byte[] bytes : List.of(new byte[] { (byte) 128 }, new byte[] { (byte) 128, 0 },
        new byte[] { (byte) 128, (byte) 128, (byte) 128, (byte) 128, 0 },
        new byte[] { (byte) 129, (byte) 128, 16 })) {
      assertThrows(IOException.class, () -> PacketCodec.decodeRemainingLength(new ByteArrayInputStream(bytes)));
    }
  }

  @Test
  void allPacketTypesRoundTripWithFragmentedReads() throws Exception {
    var will = new PublishPacket("coldchain/site/sensor/status", "offline".getBytes(StandardCharsets.UTF_8), 1, true,
        false, 0);
    List<MqttPacket> packets = List.of(
        new ConnectPacket("MQTT", 4, "sensor", true, 30, "sensor", new byte[] { 1, 2, 3 }, will),
        new ConnackPacket(false, 0), new PublishPacket("a/b", new byte[] { 0, -1, 4 }, 1, true, true, 42),
        new PubackPacket(42), new SubscribePacket(2, List.of(new SubscribePacket.Subscription("a/+", 1))),
        new SubackPacket(2, List.of(1, 128)), new UnsubscribePacket(3, List.of("a/+")),
        new UnsubackPacket(3), new PingReqPacket(), new PingRespPacket(), new DisconnectPacket());
    for (MqttPacket packet : packets) {
      var out = new ByteArrayOutputStream();
      PacketCodec.encode(packet, out);
      var fragmented = new FilterInputStream(new ByteArrayInputStream(out.toByteArray())) {
        @Override
        public int read(byte[] b, int offset, int length) throws IOException {
          return super.read(b, offset, Math.min(1, length));
        }
      };
      var decoded = PacketCodec.decode(fragmented);
      assertEquals(packet.type(), decoded.type());
      var reencoded = new ByteArrayOutputStream();
      PacketCodec.encode(decoded, reencoded);
      assertArrayEquals(out.toByteArray(), reencoded.toByteArray());
    }
  }

  @Test
  void rejectsMalformedHeadersPayloadsAndUtf8() {
    for (byte[] packet : List.of(
        new byte[] { (byte) 0x80, 0 }, new byte[] { (byte) 0xc0, 1, 0 },
        new byte[] { 0x40, 2, 0, 0 }, new byte[] { 0x30, 3, 0, 1, '#' },
        new byte[] { 0x30, 3, 0, 1, 0 }, new byte[] { 0x30, 3, 0, 1, (byte) 0xff },
        new byte[] { 0x30, 5, 0, 1, 'a' }, new byte[] { 0x36, 0 }, new byte[] { 0x50, 0 },
        new byte[] { (byte) 0x82, 2, 0, 1 })) {
      assertThrows(IOException.class, () -> PacketCodec.decode(new ByteArrayInputStream(packet)));
    }
  }

  @Test
  void capIncludesVariableHeaderAndDoesNotConsumeOversizedBody() throws Exception {
    byte[] payload = new byte[PacketCodec.MAX_PACKET_SIZE - 3];
    var out = new ByteArrayOutputStream();
    PacketCodec.encode(new PublishPacket("a", payload, 0, false, false, 0), out);
    assertEquals(payload.length,
        ((PublishPacket) PacketCodec.decode(new ByteArrayInputStream(out.toByteArray()))).payloadLength());
    assertThrows(IOException.class, () -> PacketCodec
        .encode(new PublishPacket("a", new byte[payload.length + 1], 0, false, false, 0), new ByteArrayOutputStream()));
    assertThrows(IOException.class,
        () -> PacketCodec.decode(new ByteArrayInputStream(new byte[] { 0x30, (byte) 0x81, (byte) 0x80, 16 })));
  }

  @Test
  void payloadsAreImmutableAndEofIsUnambiguous() throws Exception {
    byte[] bytes = { 1 };
    var p = new PublishPacket("a", bytes, 0, false, false, 0);
    bytes[0] = 2;
    p.payload()[0] = 3;
    assertEquals(1, p.payload()[0]);
    assertNull(PacketCodec.decode(new ByteArrayInputStream(new byte[0])));
    assertThrows(EOFException.class, () -> PacketCodec.decode(new ByteArrayInputStream(new byte[] { 0x30 })));
  }
}
