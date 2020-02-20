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
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author DaPorkchop_
 */
@RequiredArgsConstructor
final class NativeWriteBatch implements WriteBatch {
    static {
        init();
    }

    private static native void init();

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
        this.put0(key, value);
        return this;
    }

    private native void put0(byte[] key, byte[] value);

    @Override
    public synchronized WriteBatch delete(@NonNull byte[] key) {
        this.delete0(key);
        return this;
    }

    private native void delete0(byte[] key);

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
