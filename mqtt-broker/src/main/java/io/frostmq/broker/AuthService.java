package io.frostmq.broker;

import io.frostmq.common.TopicMatcher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.math.BigDecimal;
import java.util.*;

public final class AuthService {
    public static final int ITERATIONS = 210_000;
    public record Principal(String username, String role) {}
    private record Account(String role, byte[] salt, byte[] hash) {}
    private record Permissions(List<String> publish, List<String> subscribe) {}
    private final Map<String, Account> accounts;
    private final Map<String, Permissions> acl;
    private final Account dummy;

    public AuthService(Path usersPath, Path aclPath) throws IOException {
        Map<String, Object> policies = object(load(aclPath));
        var permissions = new HashMap<String, Permissions>();
        for (var entry : policies.entrySet()) {
            var policy = object(entry.getValue());
            permissions.put(entry.getKey(), new Permissions(filters(policy.get("publish")), filters(policy.get("subscribe"))));
        }
        var loaded = new HashMap<String, Account>();
        for (var entry : object(object(load(usersPath)).get("users")).entrySet()) {
            String username = entry.getKey();
            if (!TopicMatcher.validTopic(username) || username.contains("/")) throw new IOException("Invalid account username");
            var user = object(entry.getValue());
            String role = string(user.get("role"));
            if (!permissions.containsKey(role)) throw new IOException("Account references unknown ACL role");
            if (!(user.get("iterations") instanceof BigDecimal n) || n.compareTo(BigDecimal.valueOf(ITERATIONS)) != 0)
                throw new IOException("PBKDF2 iterations must be " + ITERATIONS);
            try {
                byte[] salt = Base64.getDecoder().decode(string(user.get("salt")));
                byte[] hash = Base64.getDecoder().decode(string(user.get("hash")));
                if (salt.length < 16 || hash.length != 32) throw new IOException("Invalid credential hash or salt length");
                loaded.put(username, new Account(role, salt, hash));
            } catch (IllegalArgumentException e) { throw new IOException("Invalid Base64 credential data", e); }
        }
        if (loaded.isEmpty()) throw new IOException("No configured users");
        accounts = Map.copyOf(loaded);
        acl = Map.copyOf(permissions);
        dummy = new Account("", new byte[16], new byte[32]);
    }

    public Optional<Principal> authenticate(String username, byte[] password) {
        Account user = username == null ? null : accounts.get(username);
        Account selected = user == null ? dummy : user;
        char[] chars = null;
        byte[] actual = null;
        try {
            chars = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(password == null ? new byte[0] : password)).toString().toCharArray();
            actual = hashPassword(chars, selected.salt);
            boolean equal = MessageDigest.isEqual(selected.hash, actual);
            if (user != null && password != null && equal) return Optional.of(new Principal(username, user.role));
        } catch (GeneralSecurityException | java.nio.charset.CharacterCodingException e) {
            return Optional.empty();
        } finally {
            if (chars != null) Arrays.fill(chars, '\0');
            if (actual != null) Arrays.fill(actual, (byte) 0);
        }
        return Optional.empty();
    }

    /**
     * Account usernames are reserved client IDs. This lets a device keep its identity even while
     * offline, and prevents a viewer or engine from squatting on a device ID between reconnects.
     */
    public boolean canUseClientId(Principal principal, String clientId) {
        if (principal.role().equals("device")) return clientId.equals(principal.username());
        return clientId.equals(principal.username()) || !accounts.containsKey(clientId);
    }

    public boolean canPublish(Principal principal, String topic) {
        Permissions permission = acl.get(principal.role());
        return permission != null && permission.publish.stream()
                .anyMatch(filter -> TopicMatcher.match(expand(filter, principal), topic));
    }

    public boolean canSubscribe(Principal principal, String requestedFilter) {
        Permissions permission = acl.get(principal.role());
        return permission != null && permission.subscribe.stream()
                .anyMatch(filter -> TopicMatcher.covers(expand(filter, principal), requestedFilter));
    }

    public static byte[] hashPassword(char[] password, byte[] salt) throws GeneralSecurityException {
        var spec = new PBEKeySpec(password, salt, ITERATIONS, 256);
        try { return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded(); }
        finally { spec.clearPassword(); }
    }

    private static String expand(String filter, Principal principal) { return filter.replace("${username}", principal.username()); }
    private static Object load(Path path) throws IOException {
        if (Files.size(path) > 1_048_576) throw new IOException("Configuration exceeds 1 MB");
        return ConfigJson.parse(Files.readString(path));
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) throws IOException {
        if (!(value instanceof Map<?, ?>)) throw new IOException("Expected JSON object");
        return (Map<String, Object>) value;
    }
    private static String string(Object value) throws IOException {
        if (!(value instanceof String text)) throw new IOException("Expected JSON string");
        return text;
    }
    private static List<String> filters(Object value) throws IOException {
        if (!(value instanceof List<?> list)) throw new IOException("Expected ACL filter array");
        var filters = new ArrayList<String>();
        for (Object item : list) {
            String filter = string(item);
            if (!TopicMatcher.validFilter(filter.replace("${username}", "device"))) throw new IOException("Invalid ACL filter");
            filters.add(filter);
        }
        return List.copyOf(filters);
    }
}
