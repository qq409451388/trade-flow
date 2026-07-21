package com.mtx.trade.storage.api;

/** Storage 写入失败，抛出后由本地事务回滚 metadata/blob 两次写入。 */
public class StorageWriteException extends RuntimeException {

    public StorageWriteException(String message) {
        super(message);
    }
}
