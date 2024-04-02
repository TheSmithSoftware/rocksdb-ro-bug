package com.sixgroup;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.stream.Stream;

public class Utils {
    private static final String CHECKPOINT_DIR = "checkpoints";

    public static void deleteOriginalFolder(String rocksDBPath) {
        Utils.deleteFolder(Path.of(rocksDBPath).toFile());
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

    public static Path getCheckpointFolder(Path dbPath) {
        return dbPath.resolve(CHECKPOINT_DIR);
    }

    public static String getCheckpointName(Instant instant) {
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
