package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTreeTest {
  private final List<ClientSession> sessions = new ArrayList<>();

  @AfterEach
  void closeSockets() throws IOException {
    for (ClientSession session : sessions)
      session.socket.close();
  }

  @Test
  void wildcardMatchingMatchesLinearMatcherAndCoalescesQos() throws Exception {
    var filters = List.of("#", "+", "+/+", "a", "a/#", "a/+", "a/b", "a//+", "$SYS/#", "$SYS/+",
        "+/b", "a/+/c/#", "/+", "/#", "a/b/#");
    var tree = new SubscriptionTree();
    var byFilter = new LinkedHashMap<String, ClientSession>();
    for (int i = 0; i < filters.size(); i++) {
      ClientSession session = session();
      byFilter.put(filters.get(i), session);
      assertTrue(tree.subscribe(session, filters.get(i), i % 2));
    }
    for (String topic : List.of("a", "a/", "a/b", "a/b/", "a/b/c", "a//", "$SYS/uptime", "a/$SYS",
        "/", "//", "x/y/z", "a/b/c/d", "a/x/c/d")) {
      var expected = new HashMap<ClientSession, Integer>();
      for (int i = 0; i < filters.size(); i++) {
        if (TopicMatcher.match(filters.get(i), topic))
          expected.merge(byFilter.get(filters.get(i)), i % 2, Math::max);
      }
      assertEquals(expected, tree.match(topic), topic);
    }
  }

  @Test
  void quotasRejectWithoutCreatingBranchesAndDuplicateFilterReplacesQos() throws Exception {
    var tree = new SubscriptionTree(1, 2);
    ClientSession one = session(), two = session(), three = session();
    assertTrue(tree.subscribe(one, "a/b", 0));
    assertTrue(tree.subscribe(one, "a/b", 1));
    assertEquals(1, tree.match("a/b").get(one));
    assertTrue(tree.subscribe(one, "a/b", 0));
    assertFalse(tree.subscribe(one, "a/c", 1));
    assertEquals(1, tree.filterCount());
    assertEquals(1, tree.subscriptionCount());
    assertEquals(0, tree.match("a/b").get(one));

    assertTrue(tree.subscribe(two, "a/#", 0));
    assertFalse(tree.subscribe(three, "a/+", 1));
    assertEquals(2, tree.filterCount());
    assertEquals(2, tree.subscriptionCount());
    assertFalse(tree.match("a/c").containsKey(three));
  }

  @Test
  void distinctOverlappingFiltersStillCoalesceToHighestQos() throws Exception {
    var tree = new SubscriptionTree();
    ClientSession session = session();
    assertTrue(tree.subscribe(session, "a/#", 1));
    assertTrue(tree.subscribe(session, "a/+", 0));
    assertEquals(1, tree.match("a/b").get(session));
  }

  @Test
  void unsubscribeAndSessionRemovalPruneEmptyPaths() throws Exception {
    var tree = new SubscriptionTree();
    ClientSession one = session(), two = session();
    assertTrue(tree.subscribe(one, "a/b/c", 1));
    assertTrue(tree.subscribe(one, "a/b/d", 1));
    assertTrue(tree.subscribe(two, "a/b/c", 0));
    assertEquals(2, tree.filterCount());
    assertEquals(3, tree.subscriptionCount());

    tree.unsubscribe(one, "a/b/c");
    assertEquals(2, tree.filterCount());
    assertEquals(2, tree.subscriptionCount());
    assertEquals(Map.of(two, 0), tree.match("a/b/c"));

    tree.unsubscribe(two, "a/b/c");
    assertEquals(1, tree.filterCount());
    assertEquals(1, tree.subscriptionCount());
    assertEquals(Map.of(one, 1), tree.match("a/b/d"));

    tree.remove(one);
    assertEquals(0, tree.filterCount());
    assertEquals(0, tree.subscriptionCount());
    assertEquals(Map.of(), tree.match("a/b/d"));
    assertTrue(tree.subscribe(one, "a/b/d", 1));
    assertEquals(Map.of(one, 1), tree.match("a/b/d"));
  }

  @Test
  void invalidFiltersAreRejected() throws Exception {
    var tree = new SubscriptionTree();
    assertFalse(tree.subscribe(session(), "a/#/b", 1));
    assertEquals(0, tree.filterCount());
    assertEquals(0, tree.subscriptionCount());
  }

  private ClientSession session() {
    var session = new ClientSession(null, new Socket());
    sessions.add(session);
    return session;
  }
}
