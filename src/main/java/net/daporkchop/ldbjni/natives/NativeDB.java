/*
 * Adapted from the Wizardry License
 *
 * Copyright (c) 2020-2020 DaPorkchop_ and contributors
 *
 * Permission is hereby granted to any persons and/or organizations using this software to copy, modify, merge, publish, and distribute it. Said persons and/or organizations are not allowed to use the software or any derivatives of the work for commercial use or any other means to generate income, nor are they allowed to claim this software as their own.
 *
 * The persons and/or organizations are also disallowed from sub-licensing and/or trademarking this software without explicit permission from DaPorkchop_.
 *
 * Any persons and/or organizations using this software must disclose their source code and have it publicly available, include this license, provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.ldbjni.natives;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.unsafe.PCleaner;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.File;
import java.io.IOException;

/**
 * @author DaPorkchop_
 */
final class NativeDB implements DB {
    static {
        init();
    }

    private static native void init();

    private static native long openDb(String name, boolean create_if_missing, boolean error_if_exists, boolean paranoid_checks, int write_buffer_size,
                                      int max_open_files, int block_size, int block_restart_interval, int max_file_size, int compression, long cacheSize);

    private static native long createDecompressAllocator();

    private static native void closeDb(long db, long dca);

    private final long     db;
    private final long     dca;
    private final PCleaner cleaner;

    public NativeDB(@NonNull File path, @NonNull Options options) {
        if (options.comparator() != null) {
            throw new UnsupportedOperationException("comparator");
        //} else if (options.logger() != null) {
        //    throw new UnsupportedOperationException("logger");
        } else if (options.compressionType() == null) {
            throw new NullPointerException("compressionType");
        }

        this.db = openDb(
                path.getAbsoluteFile().getAbsolutePath(),
                options.createIfMissing(),
                options.errorIfExists(),
                options.paranoidChecks(),
                options.writeBufferSize(),
                options.maxOpenFiles(),
                options.blockSize(),
                options.blockRestartInterval(),
                -1, //not present in Options...
                options.compressionType().persistentId(),
                options.cacheSize());
        this.dca = createDecompressAllocator();

        this.cleaner = PCleaner.cleaner(this, new Releaser(this.db, this.dca));
    }

    @Override
    public byte[] get(@NonNull byte[] key) throws DBException    {
        return this.get0(key, false, true, 0L);
    }

    @Override
    public byte[] get(@NonNull byte[] key, @NonNull ReadOptions options) throws DBException {
        return this.get0(key, options.verifyChecksums(), options.fillCache(), 0L); //TODO: snapshot
    }

    private native byte[] get0(byte[] key, boolean verifyChecksums, boolean fillCache, long snapshot);

    @Override
    public void put(@NonNull byte[] key, @NonNull byte[] value) throws DBException {
        this.put0(key, value, false);
    }

    @Override
    public Snapshot put(@NonNull byte[] key, @NonNull byte[] value, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.put0(key, value, options.sync());
        return null;
    }

    private native void put0(byte[] key, byte[] value, boolean sync);

    @Override
    public void delete(@NonNull byte[] key) throws DBException {
        this.delete0(key, false);
    }

    @Override
    public Snapshot delete(@NonNull byte[] key, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.delete0(key, options.sync());
        return null;
    }

    private native void delete0(byte[] key, boolean sync);

    @Override
    public WriteBatch createWriteBatch() {
        throw new UnsupportedOperationException("createWriteBatch");
    }

    @Override
    public void write(WriteBatch writeBatch) throws DBException {
        throw new UnsupportedOperationException("writeBatch");
    }

    @Override
    public Snapshot write(WriteBatch writeBatch, WriteOptions options) throws DBException {
        throw new UnsupportedOperationException("writeBatch");
    }

    @Override
    public DBIterator iterator() {
        throw new UnsupportedOperationException("iterator");
    }

    @Override
    public DBIterator iterator(@NonNull ReadOptions options) {
        throw new UnsupportedOperationException("iterator");
    }

    @Override
    public Snapshot getSnapshot() {
        throw new UnsupportedOperationException("getSnapshot");
    }

    @Override
    public long[] getApproximateSizes(@NonNull Range... ranges) {
        throw new UnsupportedOperationException("getApproximateSizes");
    }

    @Override
    public String getProperty(@NonNull String s) {
        throw new UnsupportedOperationException("getProperty");
    }

    @Override
    public void suspendCompactions() throws InterruptedException {
        throw new UnsupportedOperationException("suspendCompactions");
    }

    @Override
    public void resumeCompactions() {
        throw new UnsupportedOperationException("resumeCompactions");
    }

    @Override
    public native void compactRange(byte[] start, byte[] limit) throws DBException;

    @Override
    public void close() throws IOException {
        this.cleaner.clean();
    }

    @RequiredArgsConstructor
    private static final class Releaser implements Runnable {
        private final long db;
        private final long dca;

        @Override
        public void run() {
            closeDb(this.db, this.dca);
        }
    }
}
