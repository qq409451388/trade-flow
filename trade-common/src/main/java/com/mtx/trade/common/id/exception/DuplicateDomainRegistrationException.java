package com.mtx.trade.common.id.exception;

/**
 * 同一领域名称注册了不同实例时抛出，导致启动失败（不静默覆盖）。
 */
public class DuplicateDomainRegistrationException extends IdGenerationException {

    private final String domain;

    public DuplicateDomainRegistrationException(String domain) {
        super(String.format(
                "Duplicate domain registration for '%s': different instances detected, refusing to silently override",
                domain));
        this.domain = domain;
    }

    public String getDomain() {
        return domain;
    }
}
