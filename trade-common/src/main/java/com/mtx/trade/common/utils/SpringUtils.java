package com.mtx.trade.common.utils;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Spring运行环境工具。
 *
 * @author codex
 */
@Component
public class SpringUtils {
    private final Environment environment;

    public SpringUtils(Environment environment) {
        this.environment = environment;
    }

    /**
     * 判断当前是否为dev环境。
     *
     * @author codex
     */
    public boolean isDev() {
        return hasActiveProfile("dev");
    }

    /**
     * 判断当前是否为prod环境。
     *
     * @author codex
     */
    public boolean isProd() {
        return hasActiveProfile("prod");
    }

    /**
     * 判断指定profile是否已激活。
     *
     * @author codex
     */
    public boolean hasActiveProfile(String profile) {
        if (profile == null || profile.isBlank()) {
            return false;
        }
        return Arrays.asList(environment.getActiveProfiles()).contains(profile);
    }
}
