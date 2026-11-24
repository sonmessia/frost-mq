package io.frostmq.broker;

import org.junit.jupiter.api.*;
import java.net.Socket;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class BrokerLifecycleTest {
    @Test @Timeout(15) void closeDrainsConnectionsAndAllowsImmediateSamePortRestart() throws Exception {
        var auth = new AuthService(Path.of("../users.json"), Path.of("../acl.json"));
        int port;
        try (var first = new BrokerServer(0, auth)) {
            first.start();
            port = first.port();
            try (var socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(2_000);
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
                while (first.connectionCount() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
                assertEquals(1, first.connectionCount());
                first.close();
                assertEquals(0, first.connectionCount());
                assertTrue(socket.getInputStream().read() < 0);
            }
        }

        try (var second = new BrokerServer(port, auth)) {
            second.start();
            assertEquals(port, second.port());
        }
    }

    @Test @Timeout(10) void concurrentCloseCallsWaitForTheSameShutdown() throws Exception {
        var broker = new BrokerServer(0,
                new AuthService(Path.of("../users.json"), Path.of("../acl.json")));
        broker.start();
        Thread one = Thread.ofVirtual().start(broker::close);
        Thread two = Thread.ofVirtual().start(broker::close);
        one.join();
        two.join();
        assertFalse(one.isAlive());
        assertFalse(two.isAlive());
        assertEquals(0, broker.connectionCount());
        assertThrows(IllegalStateException.class, broker::start);
    }
}
