package io.frostmq.common;

public final class TopicMatcher {
    private TopicMatcher() {}

    public static boolean validTopic(String topic) {
        return validText(topic) && topic.indexOf('+') < 0 && topic.indexOf('#') < 0;
    }

    public static boolean validFilter(String filter) {
        if (!validText(filter)) return false;
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (level.contains("#") && (!level.equals("#") || i != levels.length - 1)) return false;
            if (level.contains("+") && !level.equals("+")) return false;
        }
        return true;
    }

    private static boolean validText(String text) {
        if (text == null || text.isEmpty() || text.indexOf('\0') >= 0) return false;
        int bytes = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i))) return false;
                bytes += 4;
            } else if (Character.isLowSurrogate(c)) return false;
            else bytes += c < 128 ? 1 : c < 2048 ? 2 : 3;
        }
        return bytes <= 65535;
    }

    public static boolean match(String topicFilter, String actualTopic) {
        if (!validFilter(topicFilter) || !validTopic(actualTopic)) return false;
        if (actualTopic.startsWith("$") && (topicFilter.startsWith("+") || topicFilter.startsWith("#"))) return false;
        String[] filter = topicFilter.split("/", -1);
        String[] topic = actualTopic.split("/", -1);
        int i = 0;
        for (; i < filter.length; i++) {
            if (filter[i].equals("#")) return true;
            if (i >= topic.length || (!filter[i].equals("+") && !filter[i].equals(topic[i]))) return false;
        }
        return i == topic.length;
    }

    /** True only when every topic requested by a subscription is within this ACL filter. */
    public static boolean covers(String allowedFilter, String requestedFilter) {
        if (!validFilter(allowedFilter) || !validFilter(requestedFilter)) return false;
        String[] allowed = allowedFilter.split("/", -1);
        String[] requested = requestedFilter.split("/", -1);
        if (requested[0].startsWith("$") && (allowed[0].equals("+") || allowed[0].equals("#"))) return false;
        int i = 0;
        for (; i < allowed.length; i++) {
            if (allowed[i].equals("#")) return true;
            if (i >= requested.length || requested[i].equals("#")) return false;
            if (!allowed[i].equals("+") && !allowed[i].equals(requested[i])) return false;
        }
        return i == requested.length;
    }
}
