package com.mtx.trade.ingress.utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class JsonPathUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonPathUtils() {
    }

    public static JsonNode parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON content", e);
        }
    }

    // ==================== JsonNode ====================

    public static JsonNode getNode(JsonNode root, String... paths) {
        if (root == null || paths == null) {
            return null;
        }

        for (String path : paths) {
            JsonNode node = getNodeByPath(root, path);
            if (node != null) {
                return node;
            }
        }

        return null;
    }

    public static String getText(JsonNode root, String... paths) {
        JsonNode node = getNode(root, paths);
        if (node == null || node.isContainerNode()) {
            return null;
        }

        String value = node.asText();
        return value == null || value.isBlank() ? null : value;
    }

    public static Long getLong(JsonNode root, String... paths) {
        JsonNode node = getNode(root, paths);
        if (node == null) {
            return null;
        }

        if (node.isIntegralNumber()) {
            return node.longValue();
        }

        String value = node.asText();
        if (value == null || value.isBlank()) {
            return null;
        }

        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Integer getInt(JsonNode root, String... paths) {
        Long value = getLong(root, paths);
        if (value == null
                || value < Integer.MIN_VALUE
                || value > Integer.MAX_VALUE) {
            return null;
        }

        return value.intValue();
    }

    public static Boolean getBoolean(JsonNode root, String... paths) {
        JsonNode node = getNode(root, paths);
        if (node == null) {
            return null;
        }

        if (node.isBoolean()) {
            return node.booleanValue();
        }

        String value = node.asText().trim().toLowerCase();

        return switch (value) {
            case "1", "true", "yes", "y" -> true;
            case "0", "false", "no", "n" -> false;
            default -> null;
        };
    }

    public static List<JsonNode> getArray(JsonNode root, String... paths) {
        JsonNode node = getNode(root, paths);
        if (node == null || !node.isArray()) {
            return Collections.emptyList();
        }

        List<JsonNode> result = new ArrayList<>(node.size());
        node.forEach(result::add);
        return result;
    }

    public static boolean exists(JsonNode root, String... paths) {
        return getNode(root, paths) != null;
    }

    // ==================== JSON原文重载 ====================

    public static JsonNode getNode(String json, String... paths) {
        return getNode(parse(json), paths);
    }

    public static String getText(String json, String... paths) {
        return getText(parse(json), paths);
    }

    public static Long getLong(String json, String... paths) {
        return getLong(parse(json), paths);
    }

    public static Integer getInt(String json, String... paths) {
        return getInt(parse(json), paths);
    }

    public static Boolean getBoolean(String json, String... paths) {
        return getBoolean(parse(json), paths);
    }

    public static List<JsonNode> getArray(String json, String... paths) {
        return getArray(parse(json), paths);
    }

    public static boolean exists(String json, String... paths) {
        return exists(parse(json), paths);
    }

    // ==================== 内部方法 ====================

    private static JsonNode getNodeByPath(JsonNode root, String path) {
        if (path == null || path.isBlank()) {
            return null;
        }

        String pointer = path.startsWith("/")
                ? path
                : toJsonPointer(path);

        JsonNode node = root.at(pointer);

        return node.isMissingNode() || node.isNull()
                ? null
                : node;
    }

    /**
     * data.orders[0].recUpdTm
     * 转换为：
     * /data/orders/0/recUpdTm
     */
    private static String toJsonPointer(String path) {
        String normalized = path.trim()
                .replaceAll("\\[(\\d+)]", ".$1");

        StringBuilder pointer = new StringBuilder();

        for (String segment : normalized.split("\\.")) {
            if (segment.isBlank()) {
                continue;
            }

            pointer.append('/')
                    .append(escapePointerSegment(segment));
        }

        return pointer.toString();
    }

    private static String escapePointerSegment(String segment) {
        return segment
                .replace("~", "~0")
                .replace("/", "~1");
    }
}