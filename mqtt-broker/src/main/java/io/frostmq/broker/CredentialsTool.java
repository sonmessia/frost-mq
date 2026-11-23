package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.*;

/** Adds/replaces one account. Password comes from the console or FROSTMQ_PASSWORD, never argv. */
public final class CredentialsTool {
    private CredentialsTool() {}
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: CredentialsTool <users.json> <username> <role>");
        String username = args[1];
        if (!TopicMatcher.validTopic(username) || username.contains("/")) throw new IllegalArgumentException("Invalid username");
        char[] password;
        String environment = System.getenv("FROSTMQ_PASSWORD");
        if (environment != null) password = environment.toCharArray();
        else if (System.console() != null) password = System.console().readPassword("Password: ");
        else throw new IllegalStateException("Set FROSTMQ_PASSWORD or run with an interactive console");
        if (password == null || password.length == 0) throw new IllegalArgumentException("Password must not be empty");
        Path path = Path.of(args[0]).toAbsolutePath();
        var root = new LinkedHashMap<String, Object>();
        var users = new LinkedHashMap<String, Object>();
        if (Files.exists(path)) {
            Object parsed = ConfigJson.parse(Files.readString(path));
            if (!(parsed instanceof Map<?, ?> map) || !(map.get("users") instanceof Map<?, ?> accounts))
                throw new IOException("Expected users JSON object");
            root.putAll((Map<String, Object>) map);
            users.putAll((Map<String, Object>) accounts);
        }
        byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        try {
            users.put(username, Map.of("role", args[2], "iterations", AuthService.ITERATIONS,
                    "salt", Base64.getEncoder().encodeToString(salt),
                    "hash", Base64.getEncoder().encodeToString(AuthService.hashPassword(password, salt))));
        } finally { Arrays.fill(password, '\0'); }
        root.put("users", users);
        Path temporary = Files.createTempFile(path.getParent(), ".users-", ".json");
        try {
            try { Files.setPosixFilePermissions(temporary, PosixFilePermissions.fromString("rw-------")); }
            catch (UnsupportedOperationException ignored) {}
            Files.writeString(temporary, json(root) + "\n");
            try { Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
            catch (AtomicMoveNotSupportedException e) { Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING); }
        } finally { Files.deleteIfExists(temporary); }
        System.out.println("Account updated. Restart the broker to reload configuration.");
    }

    private static String json(Object value) {
        if (value instanceof Map<?, ?> map) {
            var entries = new ArrayList<String>();
            map.forEach((key, item) -> entries.add(json(key.toString()) + ":" + json(item)));
            return "{" + String.join(",", entries) + "}";
        }
        if (value instanceof List<?> list) return "[" + String.join(",", list.stream().map(CredentialsTool::json).toList()) + "]";
        if (value instanceof String text) {
            var out = new StringBuilder("\"");
            for (char c : text.toCharArray()) {
                if (c == '"' || c == '\\') out.append('\\').append(c);
                else if (c < 32) out.append(String.format("\\u%04x", (int) c));
                else out.append(c);
            }
            return out.append('"').toString();
        }
        return String.valueOf(value);
    }
}
