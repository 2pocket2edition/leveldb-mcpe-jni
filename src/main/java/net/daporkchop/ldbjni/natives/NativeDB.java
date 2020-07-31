/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2020-2020 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.ldbjni.natives;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.util.internal.PlatformDependent;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.ldbjni.direct.BufType;
import net.daporkchop.ldbjni.direct.DirectDB;
import net.daporkchop.ldbjni.direct.DirectReadOptions;
import net.daporkchop.ldbjni.direct.DirectWriteBatch;
import net.daporkchop.lib.unsafe.PCleaner;
import net.daporkchop.lib.unsafe.PUnsafe;
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
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
final class NativeDB implements DirectDB {
    private static final ReadOptions DEFAULT_READ_OPTIONS = new ReadOptions();
    private static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

    private static final long CLEANER_OFFSET = PUnsafe.pork_getOffset(ByteBuffer.allocateDirect(0).getClass(), "cleaner");

    static {
        init();
    }

    private static native void init();

    private static native long openDb(String name,
                                      boolean create_if_missing, boolean error_if_exists, boolean paranoid_checks, int write_buffer_size,
                                      int max_open_files, int block_size, int block_restart_interval, int max_file_size, int compression, long cacheSize);

    private static native long createDecompressAllocator();

    private static native void closeDb(long db, long dca);

    private static native void deleteString(long string);

    private long db;
    private long dca;
    private final PCleaner cleaner;

    private final Lock readLock;
    private final Lock writeLock;

    public NativeDB(@NonNull File path, @NonNull Options options) {
        if (options.comparator() != null) {
            throw new UnsupportedOperationException("comparator");
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

        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    @Override
    public byte[] get(@NonNull byte[] key) throws DBException {
        return this.get(key, DEFAULT_READ_OPTIONS);
    }

    @Override
    public byte[] get(@NonNull byte[] key, @NonNull ReadOptions options) throws DBException {
        this.readLock.lock();
        try {
            this.assertOpen();
            return this.get0(key, options.verifyChecksums(), options.fillCache(), 0L); //TODO: snapshot
        } finally {
            this.readLock.unlock();
        }
    }

    private native byte[] get0(byte[] key, boolean verifyChecksums, boolean fillCache, long snapshot);

    @Override
    public void put(@NonNull byte[] key, @NonNull byte[] value) throws DBException {
        this.put(key, value, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot put(@NonNull byte[] key, @NonNull byte[] value, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.readLock.lock();
        try {
            this.assertOpen();
            this.put0HH(key, 0, key.length, value, 0, value.length, options.sync());
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void delete(@NonNull byte[] key) throws DBException {
        this.delete(key, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot delete(@NonNull byte[] key, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.readLock.lock();
        try {
            this.assertOpen();
            this.delete0H(key, 0, key.length, options.sync());
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public DirectWriteBatch createWriteBatch() {
        return new NativeWriteBatch(this.createWriteBatch0(), this);
    }

    private native long createWriteBatch0();

    native void releaseWriteBatch0(long ptr);

    @Override
    public void write(@NonNull WriteBatch writeBatch) throws DBException {
        this.write(writeBatch, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot write(@NonNull WriteBatch writeBatch, @NonNull WriteOptions options) throws DBException {
        if (!(writeBatch instanceof NativeWriteBatch)) {
            throw new IllegalArgumentException(writeBatch.getClass().getCanonicalName());
        } else if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.readLock.lock();
        try {
            this.assertOpen();
            synchronized (writeBatch) {
                this.writeBatch0(((NativeWriteBatch) writeBatch).ptr.get(), options.sync());
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    private native void writeBatch0(long writeBatch, boolean sync);

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
    public void compactRange(byte[] start, byte[] limit) throws DBException {
        this.readLock.lock();
        try {
            this.assertOpen();
            this.compactRange0(start, limit);
        } finally {
            this.readLock.unlock();
        }
    }

    private native void compactRange0(byte[] start, byte[] limit);

    @Override
    public void close() throws IOException {
        if (this.cleaner.hasRun()) {
            //fast-track return to avoid locking
            return;
        }
        this.writeLock.lock();
        try {
            if (!this.cleaner.hasRun()) {
                this.cleaner.clean();
                this.db = this.dca = 0L;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    //
    // direct ByteBuf methods
    //

    private ByteBufAllocator selectAlloc(@NonNull ReadOptions options) {
        ByteBufAllocator alloc = options instanceof DirectReadOptions ? ((DirectReadOptions) options).alloc() : null;
        return alloc == null ? UnpooledByteBufAllocator.DEFAULT : alloc;
    }

    private BufType selectType(@NonNull ReadOptions options) {
        BufType type = options instanceof DirectReadOptions ? ((DirectReadOptions) options).type() : null;
        return type == null ? BufType.DEFAULT : type;
    }

    @Override
    public ByteBuf get(@NonNull ByteBuf key) throws DBException {
        return this.get(key, DEFAULT_READ_OPTIONS);
    }

    @Override
    public ByteBuf get(@NonNull ByteBuf key, @NonNull ReadOptions options) throws DBException {
        this.readLock.lock();
        try {
            this.assertOpen(); //TODO: snapshot
            if (key.hasArray()) {
                return this.get0H(
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L, this.selectAlloc(options), this.selectType(options));
            } else if (key.hasMemoryAddress()) {
                return this.get0D(
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L, this.selectAlloc(options), this.selectType(options));
            } else {
                ByteBuf keyCopy = this.selectAlloc(options).ioBuffer(key.readableBytes(), key.readableBytes());
                try {
                    checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                    key.getBytes(key.readerIndex(), keyCopy);
                    return this.get(keyCopy, options);
                } finally {
                    keyCopy.release();
                }
            }
        } finally {
            this.readLock.unlock();
        }
    }

    private native ByteBuf get0H(byte[] key, int keyOff, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot, ByteBufAllocator alloc, BufType type);

    private native ByteBuf get0D(long keyAddr, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot, ByteBufAllocator alloc, BufType type);

    private ByteBuf get0_final(long valueAddr, int valueLen, @NonNull ByteBufAllocator alloc, @NonNull BufType type) {
        return type.allocate(alloc, valueLen).writeBytes(Unpooled.wrappedBuffer(valueAddr, valueLen, false));
    }

    @Override
    public boolean getInto(@NonNull ByteBuf key, @NonNull ByteBuf dst) throws DBException {
        return this.getInto(key, dst, DEFAULT_READ_OPTIONS);
    }

    @Override
    public boolean getInto(@NonNull ByteBuf key, @NonNull ByteBuf dst, @NonNull ReadOptions options) throws DBException {
        this.readLock.lock();
        try {
            this.assertOpen(); //TODO: snapshot
            if (key.hasArray()) {
                return this.getInto0H(
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L, dst);
            } else if (key.hasMemoryAddress()) {
                return this.getInto0D(
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L, dst);
            } else {
                ByteBuf keyCopy = this.selectAlloc(options).ioBuffer(key.readableBytes(), key.readableBytes());
                try {
                    checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                    key.getBytes(key.readerIndex(), keyCopy);
                    return this.getInto(keyCopy, dst, options);
                } finally {
                    keyCopy.release();
                }
            }
        } finally {
            this.readLock.unlock();
        }
    }

    private native boolean getInto0H(byte[] key, int keyOff, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot, ByteBuf dst);

    private native boolean getInto0D(long keyAddr, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot, ByteBuf dst);

    private void getInto0_final(long valueAddr, int valueLen, @NonNull ByteBuf dst) {
        dst.writeBytes(Unpooled.wrappedBuffer(valueAddr, valueLen, false));
    }

    @Override
    public ByteBuf getZeroCopy(@NonNull ByteBuf key) throws DBException {
        return this.getZeroCopy(key, DEFAULT_READ_OPTIONS);
    }

    @Override
    public ByteBuf getZeroCopy(@NonNull ByteBuf key, @NonNull ReadOptions options) throws DBException {
        this.readLock.lock();
        try {
            this.assertOpen(); //TODO: snapshot
            if (key.hasArray()) {
                return this.getZeroCopy0H(
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L);
            } else if (key.hasMemoryAddress()) {
                return this.getZeroCopy0D(
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        options.verifyChecksums(), options.fillCache(), 0L);
            } else {
                ByteBuf keyCopy = this.selectAlloc(options).ioBuffer(key.readableBytes(), key.readableBytes());
                try {
                    checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                    key.getBytes(key.readerIndex(), keyCopy);
                    return this.getZeroCopy(keyCopy, options);
                } finally {
                    keyCopy.release();
                }
            }
        } finally {
            this.readLock.unlock();
        }
    }

    private native ByteBuf getZeroCopy0H(byte[] key, int keyOff, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot);

    private native ByteBuf getZeroCopy0D(long keyAddr, int keyLen, boolean verifyChecksums, boolean fillCache, long snapshot);

    private ByteBuf getZeroCopy0_final(long valueAddr, int valueLen, long strAddr) {
        return new StdStringByteBuf(valueAddr, valueLen, strAddr);
    }

    @Override
    public void put(@NonNull ByteBuf key, @NonNull ByteBuf value) throws DBException {
        this.put(key, value, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot put(@NonNull ByteBuf key, @NonNull ByteBuf value, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.readLock.lock();
        try {
            this.assertOpen();
            if (key.hasArray()) {
                if (value.hasArray()) {
                    this.put0HH(
                            key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                            value.array(), value.arrayOffset() + value.readerIndex(), value.readableBytes(),
                            options.sync());
                    return null;
                } else if (value.hasMemoryAddress()) {
                    this.put0HD(
                            key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                            value.memoryAddress() + value.readerIndex(), value.readableBytes(),
                            options.sync());
                    return null;
                }
            } else if (value.hasMemoryAddress())    {
                if (value.hasArray()) {
                    this.put0DH(
                            key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                            value.array(), value.arrayOffset() + value.readerIndex(), value.readableBytes(),
                            options.sync());
                    return null;
                } else if (value.hasMemoryAddress()) {
                    this.put0DD(
                            key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                            value.memoryAddress() + value.readerIndex(), value.readableBytes(),
                            options.sync());
                    return null;
                }
            }
            if (!key.hasArray() && !key.hasMemoryAddress()) {
                ByteBuf keyCopy = UnpooledByteBufAllocator.DEFAULT.buffer(key.readableBytes(), key.readableBytes());
                try {
                    checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                    key.getBytes(key.readerIndex(), keyCopy);
                    return this.put(keyCopy, value, options);
                } finally {
                    keyCopy.release();
                }
            } else if (!value.hasArray() && !value.hasMemoryAddress()) {
                ByteBuf valueCopy = UnpooledByteBufAllocator.DEFAULT.buffer(value.readableBytes(), value.readableBytes());
                try {
                    checkState(valueCopy.hasArray() || valueCopy.hasMemoryAddress(), valueCopy);
                    value.getBytes(value.readerIndex(), valueCopy);
                    return this.put(key, valueCopy, options);
                } finally {
                    valueCopy.release();
                }
            } else {
                throw new IllegalArgumentException(key + " " + value);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    private native void put0HH(byte[] key, int keyOff, int keyLen, byte[] val, int valOff, int valLen, boolean sync);

    private native void put0HD(byte[] key, int keyOff, int keyLen, long valAddr, int valLen, boolean sync);

    private native void put0DH(long keyAddr, int keyLen, byte[] val, int valOff, int valLen, boolean sync);

    private native void put0DD(long keyAddr, int keyLen, long valAddr, int valLen, boolean sync);

    @Override
    public void delete(@NonNull ByteBuf key) throws DBException {
        this.delete(key, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot delete(@NonNull ByteBuf key, @NonNull WriteOptions options) throws DBException {
        if (options.snapshot()) {
            throw new UnsupportedOperationException("snapshot");
        }

        this.readLock.lock();
        try {
            this.assertOpen();
            if (key.hasArray()) {
                this.delete0H(
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        options.sync());
            } else if (key.hasMemoryAddress()) {
                this.delete0D(
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        options.sync());
            } else {
                ByteBuf keyCopy = UnpooledByteBufAllocator.DEFAULT.buffer(key.readableBytes(), key.readableBytes());
                try {
                    checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                    key.getBytes(key.readerIndex(), keyCopy);
                    return this.delete(keyCopy, options);
                } finally {
                    keyCopy.release();
                }
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    private native void delete0H(byte[] key, int keyOff, int keyLen, boolean sync);

    private native void delete0D(long keyAddr, int keyLen, boolean sync);

    private void assertOpen() {
        if (this.db == 0L) {
            throw new IllegalStateException("NativeDB already closed!");
        }
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

    private static final class StdStringByteBuf extends UnpooledUnsafeDirectByteBuf {
        private static final long DONOTFREE_OFFSET = PUnsafe.pork_getOffset(UnpooledUnsafeDirectByteBuf.class, "doNotFree");

        protected final long strAddr;

        public StdStringByteBuf(long valueAddr, int valueLen, long strAddr) {
            super(UnpooledByteBufAllocator.DEFAULT, PlatformDependent.directBuffer(valueAddr, valueLen), valueLen);

            PUnsafe.putBoolean(this, DONOTFREE_OFFSET, false);

            this.strAddr = strAddr;
        }

        @Override
        protected void freeDirect(ByteBuffer buffer) {
            deleteString(this.strAddr);
        }
    }
}
