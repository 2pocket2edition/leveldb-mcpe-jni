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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.daporkchop.lib.unsafe.PCleaner;
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class NativeWriteBatch implements WriteBatch {
    final AtomicLong ptr;

    private final NativeDB db;
    private final PCleaner cleaner;

    public NativeWriteBatch(long ptr, @NonNull NativeDB db) {
        this.ptr = new AtomicLong(ptr);
        this.db = db;
        this.cleaner = PCleaner.cleaner(this, new Releaser(this.ptr, this.db));
    }

    @Override
    public synchronized WriteBatch put(@NonNull byte[] key, @NonNull byte[] value) {
        this.put0(this.ptr.get(), key, value);
        return this;
    }

    private native void put0(long ptr, byte[] key, byte[] value);

    @Override
    public synchronized WriteBatch delete(@NonNull byte[] key) {
        this.delete0(this.ptr.get(), key);
        return this;
    }

    private native void delete0(long ptr, byte[] key);

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
