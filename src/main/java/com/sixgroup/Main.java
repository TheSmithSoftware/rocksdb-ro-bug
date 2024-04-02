package com.sixgroup;

import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.stream.Stream;

public class Main {
    private static final String CHECKPOINT_DIR = "checkpoints";

    public static void main(String[] args) {
        Integer i = 1;
        Integer ii = 1;
        final String rocksDBPath = "c:\\Develop\\data\\rocksdb-sandbox";
        deleteOriginalFolder(rocksDBPath);
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setEnableBlobFiles(true);
        options.setMinBlobSize(256); // references are stored directly
        options.setBlobCompressionType(CompressionType.ZLIB_COMPRESSION);
        byte[] messageKey = "id".getBytes(StandardCharsets.US_ASCII);
//        byte[] message = "message".getBytes(StandardCharsets.US_ASCII);
        byte[] message = ("{\n" +
                "  \"$schema\": \"../schema/fixedincome.v1.json\",\n" +
                "  \"metadata\": {\n" +
                "    \"referenceTimestamp\": \"2024-01-02T12:00:00.000000Z\"\n" +
                "  },\n" +
                "  \"dataSetId\": 0,\n" +
                "  \"identifiers\": [\n" +
                "    {\n" +
                "      \"isin\": \"CH40000\",\n" +
                "      \"dataSetId\": 1234,\n" +
                "      \"primary\": true\n" +
                "    },\n" +
                "    {\n" +
                "      \"cusip\": \"someCusip\",\n" +
                "      \"linked\": true,\n" +
                "      \"dataSetId\": 1234\n" +
                "    },\n" +
                "    {\n" +
                "      \"valor\": \"someValor\",\n" +
                "      \"linked\": true,\n" +
                "      \"dataSetId\": 5678\n" +
                "    }\n" +
                "  ],\n" +
                "  \"issuer\": {\n" +
                "    \"identifiers\": [\n" +
                "      {\n" +
                "        \"lei\": \"969500CJCTMI93QJKK89\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"domicile\": \"FR\",\n" +
                "    \"supplier:entityName\": \"AXA Banque SA\"\n" +
                "  },\n" +
                "  \"issueDate\": \"2024-01-01\",\n" +
                "  \"couponType\": \"FIXED\"\n" +
                "}").getBytes(StandardCharsets.US_ASCII);


        options.setEnableBlobFiles(false);

        System.out.println("Without blobDb: " + options.enableBlobFiles());
        Path checkpointPath = writeMessage(options, rocksDBPath, messageKey, message, options.enableBlobFiles());
        readMessage(options, messageKey, checkpointPath);

        options.setEnableBlobFiles(true);
        System.out.println("With blobDb: " + options.enableBlobFiles());
        checkpointPath = writeMessage(options, rocksDBPath, messageKey, message, options.enableBlobFiles());
        readMessage(options, messageKey, checkpointPath);

    }

    private static void deleteOriginalFolder(String rocksDBPath) {
        deleteFolder(Path.of(rocksDBPath).toFile());
        deleteFolder(Path.of(rocksDBPath + "Blob").toFile());
    }

    private static void readMessage(Options options, byte[] messageKey, Path checkpointPath) {
        RocksDB rocksDB;
        try {
            rocksDB = RocksDB.openReadOnly(options, String.valueOf(checkpointPath/*"c:\\Develop\\data\\rocksdb-sandbox\\checkpoints\\2024-03-26T15_17_48.859819100Z"*/));
        } catch (RocksDBException ex) {
            throw new RuntimeException(ex);
        }

        byte[] deserializedMessage;

        try {
            deserializedMessage = rocksDB.get(messageKey);
            if (deserializedMessage != null) {
                System.out.println(new String(deserializedMessage, StandardCharsets.US_ASCII));
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static Path writeMessage(
        Options options, String rocksDBPath, byte[] messageKey, byte[] message, boolean isBlobDB
    ) {
        Path checkpointPath;
        String realRocksDbPath = getRocksDbPath(rocksDBPath, isBlobDB);
        try (RocksDB rocksDB = RocksDB.open(options, realRocksDbPath)) {
            try (WriteOptions writeOptions = new WriteOptions()) {
                rocksDB.put(writeOptions, messageKey, message);
            }
            checkpointPath = createCheckpoint(rocksDB, Path.of(realRocksDbPath));
            System.out.println(checkpointPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return checkpointPath;
    }

    private static String getRocksDbPath(String rocksDBPath, boolean isBlobDB) {
        return isBlobDB ? rocksDBPath + "Blob" : rocksDBPath;
    }

    public static Path createCheckpoint(RocksDB rocksDB, Path dbPath) throws RocksDBException, IOException {
        final Path checkpointDir = getCheckpointFolder(dbPath);
        Files.createDirectories(checkpointDir);

        final Instant instant = Instant.now();
        final Path checkpointPath = checkpointDir.resolve(getCheckpointName(instant));
        if (Files.isDirectory(checkpointPath)) {
            try (final Stream<Path> directoryStream = Files.list(checkpointPath)) {
                if (directoryStream.findFirst().isPresent()) {
                    return null;
                }
            }
        }

        try (Checkpoint checkpoint = Checkpoint.create(rocksDB)) {
            checkpoint.createCheckpoint(checkpointPath.toAbsolutePath().toString());
        }

        return checkpointPath;
    }

    private static Path getCheckpointFolder(Path dbPath) {
        return dbPath.resolve(CHECKPOINT_DIR);
    }

    private static String getCheckpointName(Instant instant) {
        return instant.toString().replace(':', '_'); // windows friendly file name
    }

    public static boolean deleteFolder(File folder) {
        if (folder == null || !folder.isDirectory()) {
            return false;
        }
        // recursively delete the folder content silently accepting that some files/folders might not be deleteable
        final File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // delete any subfolders
                    deleteFolder(file);
                } else {
                    deleteFile(file.toPath());
                }
            }
        }
        // delete the (now empty) folder
        return deleteFile(folder.toPath());
    }

    /**
     * Delete a file or (empty) folder.
     *
     * @param path File or folder to delete, silently ignored when null or non-existant
     * @return Success flag (i.e. the file does not or never did exist)
     */
    public static boolean deleteFile(Path path) {
        if (path != null && Files.exists(path)) {
            final File file = path.toFile();
            try {
                if (!file.setWritable(true)) {     // Windows cannot delete read-only files...
                    System.out.println(MessageFormat.format("Failed to set write permission for {0}", path));
                }
                if (Files.deleteIfExists(path)) {
                    System.out.println(MessageFormat.format("Deleted {0}", file.getAbsoluteFile()));
                }
            } catch (IOException ioex) {
                System.out.println(MessageFormat.format("IOException while deleting {0}", path));
                throw new RuntimeException(ioex);
            }
            if (Files.exists(path)) {
                System.out.println(MessageFormat.format("File {0} ({1} bytes) still exists", path, file.length()));
                return false;
            }
        }
        return true;
    }
}