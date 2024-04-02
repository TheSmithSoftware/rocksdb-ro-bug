package com.sixgroup;

import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class RocksDBUtils {
    public static final int BLOB_MIN_SIZE = 256;
    public static Options createOptions() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setEnableBlobFiles(true);
        options.setMinBlobSize(BLOB_MIN_SIZE);
        options.setBlobCompressionType(CompressionType.ZLIB_COMPRESSION);
        return options;
    }

    public static void readMessage(Options options, byte[] messageKey, Path checkpointPath) {
        RocksDB rocksDB;
        try {
            rocksDB = RocksDB.openReadOnly(options, String.valueOf(checkpointPath));
        } catch (RocksDBException ex) {
            throw new RuntimeException(ex);
        }

        byte[] deserializedMessage;

        try {
            deserializedMessage = rocksDB.get(messageKey);
            if (deserializedMessage != null) {
                System.out.println(new String(deserializedMessage, StandardCharsets.US_ASCII));
            } else {
                System.out.println("Message not found!");
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path writeMessage(Options options, String rocksDBPath, byte[] messageKey, byte[] message) {
        Path checkpointPath;
        try (RocksDB rocksDB = RocksDB.open(options, rocksDBPath)) {
            try (WriteOptions writeOptions = new WriteOptions()) {
                rocksDB.put(writeOptions, messageKey, message);
            }
            checkpointPath = Utils.createCheckpoint(rocksDB, Path.of(rocksDBPath));
            System.out.println("Checkpoint's path: " + checkpointPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return checkpointPath;
    }
}
