package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.storage.api.StorageRef;

/** Storage 成功、event 失败场景的审计服务。 */
public interface EventIngestFailureLogService {

    String STAGE_SIGNATURE_VERIFY = "SIGNATURE_VERIFY";
    String STAGE_STORAGE_PERSIST = "STORAGE_PERSIST";
    String STAGE_EVENT_FIELD_PARSE = "EVENT_FIELD_PARSE";
    String STAGE_EVENT_PERSIST = "EVENT_PERSIST";

    Long recordFailure(
            String requestId,
            int sourceSystem,
            int contentType,
            byte[] payloadSha256,
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure);
}
