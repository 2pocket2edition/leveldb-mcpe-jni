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
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.ldbjni.direct.DirectWriteBatch;
import net.daporkchop.lib.unsafe.PCleaner;
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static net.daporkchop.lib.common.util.PValidation.checkState;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class NativeWriteBatch implements DirectWriteBatch {
    final AtomicLong ptr;

    private final NativeDB db;
    private final PCleaner cleaner;

    public NativeWriteBatch(long ptr, @NonNull NativeDB db) {
        this.ptr = new AtomicLong(ptr);
        this.db = db;
        this.cleaner = PCleaner.cleaner(this, new Releaser(this.ptr, this.db));
    }

    @Override
    public synchronized DirectWriteBatch put(@NonNull byte[] key, @NonNull byte[] value) {
        this.put0HH(this.ptr.get(), key, 0, key.length, value, 0, value.length);
        return this;
    }

    @Override
    public synchronized DirectWriteBatch put(@NonNull ByteBuf key, @NonNull ByteBuf value) {
        if (key.hasArray()) {
            if (value.hasArray()) {
                this.put0HH(
                        this.ptr.get(),
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        value.array(), value.arrayOffset() + value.readerIndex(), value.readableBytes());
                return this;
            } else if (value.hasMemoryAddress()) {
                this.put0HD(
                        this.ptr.get(),
                        key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes(),
                        value.memoryAddress() + value.readerIndex(), value.readableBytes());
                return this;
            }
        } else if (key.hasMemoryAddress())    {
            if (value.hasArray()) {
                this.put0DH(
                        this.ptr.get(),
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        value.array(), value.arrayOffset() + value.readerIndex(), value.readableBytes());
                return this;
            } else if (value.hasMemoryAddress()) {
                this.put0DD(
                        this.ptr.get(),
                        key.memoryAddress() + key.readerIndex(), key.readableBytes(),
                        value.memoryAddress() + value.readerIndex(), value.readableBytes());
                return this;
            }
        }
        if (!key.hasArray() && !key.hasMemoryAddress()) {
            ByteBuf keyCopy = ByteBufAllocator.DEFAULT.buffer(key.readableBytes(), key.readableBytes());
            try {
                checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                key.getBytes(key.readerIndex(), keyCopy);
                this.put(keyCopy, value);
            } finally {
                keyCopy.release();
            }
        } else if (!value.hasArray() && !value.hasMemoryAddress()) {
            ByteBuf valueCopy = ByteBufAllocator.DEFAULT.buffer(value.readableBytes(), value.readableBytes());
            try {
                checkState(valueCopy.hasArray() || valueCopy.hasMemoryAddress(), valueCopy);
                value.getBytes(value.readerIndex(), valueCopy);
                this.put(key, valueCopy);
            } finally {
                valueCopy.release();
            }
        } else {
            throw new IllegalArgumentException(key + " " + value);
        }
        return this;
    }

    private native void put0HH(long ptr, byte[] key, int keyOff, int keyLen, byte[] val, int valOff, int valLen);

    private native void put0HD(long ptr, byte[] key, int keyOff, int keyLen, long valAddr, int valLen);

    private native void put0DH(long ptr, long keyAddr, int keyLen, byte[] val, int valOff, int valLen);

    private native void put0DD(long ptr, long keyAddr, int keyLen, long valAddr, int valLen);

    @Override
    public synchronized DirectWriteBatch delete(@NonNull byte[] key) {
        this.delete0H(this.ptr.get(), key, 0, key.length);
        return this;
    }

    @Override
    public synchronized DirectWriteBatch delete(@NonNull ByteBuf key) {
        if (key.hasArray()) {
            this.delete0H(
                    this.ptr.get(),
                    key.array(), key.arrayOffset() + key.readerIndex(), key.readableBytes());
        } else if (key.hasMemoryAddress()) {
            this.delete0D(
                    this.ptr.get(),
                    key.memoryAddress() + key.readerIndex(), key.readableBytes());
        } else {
            ByteBuf keyCopy = ByteBufAllocator.DEFAULT.buffer(key.readableBytes(), key.readableBytes());
            try {
                checkState(keyCopy.hasArray() || keyCopy.hasMemoryAddress(), keyCopy);
                key.getBytes(key.readerIndex(), keyCopy);
                this.delete(keyCopy);
            } finally {
                keyCopy.release();
            }
        }
        return this;
    }

    private native void delete0H(long ptr, byte[] key, int keyOff, int keyLen);

    private native void delete0D(long ptr, long keyAddr, int keyLen);

    @Override
    public synchronized void close() throws IOException {
        this.cleaner.clean();
    }

    @RequiredArgsConstructor
    private static final class Releaser implements Runnable {
        @NonNull
        private final AtomicLong     ptr;
        @NonNull
        private final NativeDB db;

        @Override
        public void run() {
            this.db.releaseWriteBatch0(this.ptr.getAndSet(0L));
        }
    }
}
