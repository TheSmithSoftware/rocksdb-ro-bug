package com.sixgroup;

import org.rocksdb.Options;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    private static final String DEFAULT_ROCKSDB_DIRECTORY =
        Paths.get("").resolve("temp").toAbsolutePath().toString();

    public static void main(String[] args) {
        final String rocksDBPath = getRocksDBPath(args);
        Utils.deleteOriginalFolder(rocksDBPath);
        Options options = RocksDBUtils.createOptions();
        byte[] messageKey = "id".getBytes(StandardCharsets.US_ASCII);
        byte[] message = loadMessage();

        System.out.println("First write:");
        Path checkpointPath = RocksDBUtils.writeMessage(options, rocksDBPath, messageKey, message);
        System.out.println("First read:");
        RocksDBUtils.readMessage(options, messageKey, checkpointPath);
        System.out.println("Second write:");
        checkpointPath = RocksDBUtils.writeMessage(options, rocksDBPath, messageKey, message);
        System.out.println("Second read:");
        RocksDBUtils.readMessage(options, messageKey, checkpointPath);
    }

    private static byte[] loadMessage() {
        byte[] message;
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("test_message.json")) {
            assert inputStream != null;
            message = inputStream.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assert message.length > RocksDBUtils.BLOB_MIN_SIZE;
        return message;
    }

    private static String getRocksDBPath(String[] args) {
        String argsRocksDBPath = null;
        if (args != null && args.length > 0) {
            argsRocksDBPath = args[0];
        }
        return argsRocksDBPath != null ? argsRocksDBPath : DEFAULT_ROCKSDB_DIRECTORY;
    }
}