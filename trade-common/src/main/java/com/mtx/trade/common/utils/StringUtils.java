package com.mtx.trade.common.utils;

public final class StringUtils {
    private StringUtils() {
    }

    public static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public static boolean isNotBlank(String value) {
        return !isBlank(value);
    }

    public static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    /*public static String firstNotBlank(String first, String second) {
        return isNotBlank(first) ? first : second;
    }*/

    public static boolean isRemoteUrl(String value) {
        return isNotBlank(value)
                && (value.startsWith("http://") || value.startsWith("https://"));
    }

    public static String firstNotBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (!StringUtils.isBlank(value)) {
                return value.trim();
            }
        }
        return "";
    }

    public static String trimToNull(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return value.trim();
    }

    /**
     * 判断文本是否包含中文字符。
     *
     * @author codex
     */
    public static boolean containsChinese(String value) {
        return isNotBlank(value) && value.matches(".*\\p{IsHan}.*");
    }


    public static String extractEnText(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        return text.replaceAll("[^a-zA-Z]", "");
    }

    /**
     * 提取文本中的中文字符。
     *
     * @author codex
     */
    public static String extractChineseText(String value) {
        if (isBlank(value)) {
            return null;
        }
        return trimToNull(value.replaceAll("[^\\p{IsHan}]", ""));
    }

    public static String sanitizeFilenamePart(String part) {
        if (part == null || part.isEmpty()) {
            return "";
        }
        return part.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

     public static String orDefault(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value;
     }

     public static String orWu(String value) {
        return orDefault(value, "无");
     }

    /**
     * 从输入字符串中提取第一个匹配的中国大陆手机号（11位，1开头，第二位3-9）
     * @param input 原始字符串（可能包含空格、分隔符、汉字等）
     * @return 提取到的纯数字手机号，若未匹配则返回 null
     */
    public static String extractPhoneNumber(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }
        // 正则说明：
        // 1[3-9]\\d{9} 匹配以1开头，第二位为3-9，后跟9位数字，共11位
        // 如需支持带分隔符（如 138-0013-8000），请改用更复杂的正则，此处按纯数字匹配
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("1[3-9]\\d{9}");
        java.util.regex.Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }
}
