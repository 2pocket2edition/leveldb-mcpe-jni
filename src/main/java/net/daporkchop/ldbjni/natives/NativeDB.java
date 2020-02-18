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
    private static native long openDb(String name);

    private static native void closeDb(long db);

    private final long     db;
    private final PCleaner cleaner;

    public NativeDB(@NonNull File path, @NonNull Options options) {
        this.db = openDb(path.getAbsoluteFile().getAbsolutePath());
        this.cleaner = PCleaner.cleaner(this, new Releaser(this.db));
    }

    @Override
    public byte[] get(byte[] bytes) throws DBException {
        return new byte[0];
    }

    @Override
    public byte[] get(byte[] bytes, ReadOptions readOptions) throws DBException {
        return new byte[0];
    }

    @Override
    public DBIterator iterator() {
        return null;
    }

    @Override
    public DBIterator iterator(ReadOptions readOptions) {
        return null;
    }

    @Override
    public void put(byte[] bytes, byte[] bytes1) throws DBException {
    }

    @Override
    public void delete(byte[] bytes) throws DBException {
    }

    @Override
    public void write(WriteBatch writeBatch) throws DBException {
    }

    @Override
    public WriteBatch createWriteBatch() {
        return null;
    }

    @Override
    public Snapshot put(byte[] bytes, byte[] bytes1, WriteOptions writeOptions) throws DBException {
        return null;
    }

    @Override
    public Snapshot delete(byte[] bytes, WriteOptions writeOptions) throws DBException {
        return null;
    }

    @Override
    public Snapshot write(WriteBatch writeBatch, WriteOptions writeOptions) throws DBException {
        return null;
    }

    @Override
    public Snapshot getSnapshot() {
        return null;
    }

    @Override
    public long[] getApproximateSizes(Range... ranges) {
        return new long[0];
    }

    @Override
    public String getProperty(String s) {
        return null;
    }

    @Override
    public void suspendCompactions() throws InterruptedException {
    }

    @Override
    public void resumeCompactions() {
    }

    @Override
    public void compactRange(byte[] bytes, byte[] bytes1) throws DBException {
    }

    @Override
    public void close() throws IOException {
        this.cleaner.clean();
    }

    @RequiredArgsConstructor
    private static final class Releaser implements Runnable {
        private final long db;

        @Override
        public void run() {
            closeDb(this.db);
        }
    }
}
