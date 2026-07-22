package com.mtx.trade.storage.api;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageDtoValidationTest {

    @Test
    void shouldRejectNullStorageContent() {
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> new StorageWriteCommand(1, 1, null, LocalDateTime.now())),
                "content");
    }

    @Test
    void shouldRejectInvalidStorageReference() {
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> new StorageRef(0L, new byte[32], 1)),
                "storageId");
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> new StorageRef(1L, new byte[31], 1)),
                "sha256");
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> new StorageRef(1L, new byte[32], -1)),
                "contentLength");
    }

    @Test
    void shouldRejectInvalidStorageMetadata() {
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> metadata(null, new byte[32], 1)),
                "storageId");
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> metadata(1L, null, 1)),
                "sha256");
        assertMessageContains(
                assertThrows(IllegalArgumentException.class,
                        () -> metadata(1L, new byte[32], -1)),
                "payloadLength");
    }

    @Test
    void shouldDefensivelyCopySha256() {
        byte[] sha256 = new byte[32];
        StorageRef ref = new StorageRef(1L, sha256, 1);
        StorageMetadata metadata = metadata(1L, sha256, 1);

        sha256[0] = 1;
        assertEquals(0, ref.sha256()[0]);
        assertEquals(0, metadata.sha256()[0]);

        byte[] returned = ref.sha256();
        returned[0] = 2;
        assertEquals(0, ref.sha256()[0]);
    }

    private static StorageMetadata metadata(Long storageId, byte[] sha256, int payloadLength) {
        return new StorageMetadata(storageId, 1, 1, sha256, payloadLength, LocalDateTime.now());
    }

    private static void assertMessageContains(IllegalArgumentException exception, String expected) {
        assertTrue(exception.getMessage().contains(expected));
    }
}
