package com.slack.kaldb.logstore;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class WrappingMMapDirectory extends MMapDirectory {
  private final Method maybeDeletePendingFilesMethod;
  private final Method privateDeleteFileMethod;

  private final Field pendingDeletesField;

  private final Field nextTempFileCounterField;

  private final AtomicLong atomicLong = new AtomicLong(0);

  public WrappingMMapDirectory(Path path) throws IOException {
    super(path);

    try {
      maybeDeletePendingFilesMethod = FSDirectory.class.getDeclaredMethod("maybeDeletePendingFiles");
      maybeDeletePendingFilesMethod.setAccessible(true);

      privateDeleteFileMethod = FSDirectory.class.getDeclaredMethod("privateDeleteFile", String.class, boolean.class);
      privateDeleteFileMethod.setAccessible(true);

      pendingDeletesField = FSDirectory.class.getDeclaredField("pendingDeletes");
      pendingDeletesField.setAccessible(true);

      nextTempFileCounterField = FSDirectory.class.getDeclaredField("nextTempFileCounter");
      nextTempFileCounterField.setAccessible(true);
    } catch (NoSuchMethodException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    maybeDeletePendingFiles();
    // If this file was pending delete, we are now bringing it back to life:
    if (getPendingDeletes().remove(name)) {
      privateDeleteFile(name, true); // try again to delete it - this is best effort
      getPendingDeletes().remove(name); // watch out - if the delete fails it put
    }
    return new WrappingMMapDirectory.FSIndexOutput(name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    ensureOpen();
    maybeDeletePendingFiles();
    while (true) {
      try {
        String name = getTempFileName(prefix, suffix, getNextTempFileCounter().getAndIncrement());
        if (getPendingDeletes().contains(name)) {
          continue;
        }
        return new WrappingMMapDirectory.FSIndexOutput(name,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
      } catch (FileAlreadyExistsException faee) {
        // Retry with next incremented name
      }
    }
  }

  private void maybeDeletePendingFiles() {
    try {
      maybeDeletePendingFilesMethod.invoke(this);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("JavaReflectionInvocation")
  private void privateDeleteFile(String name, boolean isPendingDelete) {
    try {
      privateDeleteFileMethod.invoke(name, isPendingDelete);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Set<String> getPendingDeletes() {
    try {
      return (Set<String>) pendingDeletesField.get(this);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private AtomicLong getNextTempFileCounter() {
    try {
      return (AtomicLong) nextTempFileCounterField.get(this);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public long getSize() {
    return atomicLong.get();
  }

  final class FSIndexOutput extends OutputStreamIndexOutput {
    /**
     * The maximum chunk size is 8192 bytes, because file channel mallocs
     * a native buffer outside of stack if the write buffer size is larger.
     */
    static final int CHUNK_SIZE = 8192;

    public FSIndexOutput(String name) throws IOException {
      this(name, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    FSIndexOutput(String name, OpenOption... options) throws IOException {
      super("FSIndexOutput(path=\"" + directory.resolve(name) + "\")", name, new FilterOutputStream(Files.newOutputStream(directory.resolve(name), options)) {
        // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
          while (length > 0) {
            atomicLong.addAndGet(length);
            final int chunk = Math.min(length, CHUNK_SIZE);
            out.write(b, offset, chunk);
            length -= chunk;
            offset += chunk;
          }
        }
      }, CHUNK_SIZE);
    }
  }
}
