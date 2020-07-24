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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import net.daporkchop.ldbjni.LevelDB;
import net.daporkchop.ldbjni.direct.BufType;
import net.daporkchop.ldbjni.direct.DirectDB;
import net.daporkchop.ldbjni.direct.DirectReadOptions;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.Tuple;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.encoding.ToBytes;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static net.daporkchop.lib.common.util.PValidation.*;

/**
 * @author DaPorkchop_
 */
public class LevelDBTest {
    public static final File TEST_ROOT = new File("test_out");

    @BeforeClass
    public static void ensureNative() {
        if (!LevelDB.PROVIDER.isNative()) {
            throw new IllegalStateException("Not using native LevelDB!");
        }
    }

    @Before
    public void nukeTestDir() {
        if (PFiles.checkDirectoryExists(TEST_ROOT)) {
            System.out.println("Nuking " + TEST_ROOT);
            PFiles.rmContentsParallel(TEST_ROOT);
        }
    }

    @Test
    public void testManyWrites() throws IOException {
        this.doTest(db -> {
            int cnt = 100000;
            int batchSize = 1000;
            IntStream.range(0, cnt / batchSize)
                    .forEach(i -> {
                        try (WriteBatch writeBatch = db.createWriteBatch()) {
                            for (int j = 0; j < batchSize; j++) {
                                byte[] arr = new byte[ThreadLocalRandom.current().nextInt(10, 100000)];
                                writeBatch.put(ToBytes.toBytes(i * batchSize + j), arr);
                            }

                            db.write(writeBatch);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println(i);
                    });
        }, CompressionType.NONE);
    }

    @Test
    public void testManyReads() throws IOException {
        this.doTest(db -> {
            //write a single large entry
            db.put(ToBytes.toBytes(0), new byte[1 << 20]);

            //compact it
            db.compactRange(null, null);

            //get it a bunch of times to see if the byte[]s are actually being GC-d
            IntStream.range(0, 10000 * 2).parallel()
                    .peek(i -> {
                        if ((i & 511) == 0) {
                            System.gc();
                        }
                    })
                    .forEach(i -> db.get(ToBytes.toBytes(0)));
        }, CompressionType.SNAPPY);
    }

    @Test
    public void testDirectRead() throws IOException {
        this.doTest(db -> {
            //write a single large entry
            byte[] arr0 = new byte[1 << 20 >> 3];
            ThreadLocalRandom.current().nextBytes(arr0);
            byte[] arr1 = new byte[arr0.length];
            db.put(ToBytes.toBytes(0), arr0);
            db.put(ToBytes.toBytes(1), arr1);

            //compact it
            db.compactRange(null, null);

            { //sanity checks
                ByteBuf buf = ((DirectDB) db).get(Unpooled.wrappedBuffer(ToBytes.toBytes(0)));
                try {
                    System.out.println(buf);
                    this.checkIdentical(arr0, buf);
                } finally {
                    buf.release();
                }
                buf = ((DirectDB) db).get(Unpooled.wrappedBuffer(ToBytes.toBytes(1)));
                try {
                    System.out.println(buf);
                    this.checkIdentical(arr1, buf);
                } finally {
                    buf.release();
                }

                buf = ((DirectDB) db).get(Unpooled.directBuffer().writeBytes(ToBytes.toBytes(0)),
                        new DirectReadOptions().alloc(PooledByteBufAllocator.DEFAULT).type(BufType.HEAP));
                try {
                    System.out.println(buf);
                    this.checkIdentical(arr0, buf);
                } finally {
                    buf.release();
                }
                buf = ((DirectDB) db).get(Unpooled.directBuffer().writeBytes(ToBytes.toBytes(1)),
                        new DirectReadOptions().alloc(PooledByteBufAllocator.DEFAULT).type(BufType.HEAP));
                try {
                    System.out.println(buf);
                    this.checkIdentical(arr1, buf);
                } finally {
                    buf.release();
                }
            }

            { //get it a bunch of times to check for memory leaks
                ByteBuf key0 = Unpooled.directBuffer().writeBytes(ToBytes.toBytes(0));
                ByteBuf key1 = Unpooled.directBuffer().writeBytes(ToBytes.toBytes(1));
                System.out.println("get");
                IntStream.range(0, 10000).parallel()
                        .mapToObj(i -> new Tuple<>(((DirectDB) db).get(key0), ((DirectDB) db).get(key1)))
                        .peek(t -> this.checkIdentical(arr0, t.getA()))
                        .peek(t -> this.checkIdentical(arr1, t.getB()))
                        .forEach(t -> {
                            t.getA().release();
                            t.getB().release();
                        });

                System.out.println("getInto");
                ThreadLocal<Tuple<ByteBuf, ByteBuf>> tl = ThreadLocal.withInitial(() ->
                        new Tuple<>(Unpooled.directBuffer(arr0.length, arr0.length), Unpooled.directBuffer(arr1.length, arr1.length)));
                IntStream.range(0, 10000).parallel()
                        .mapToObj(i -> tl.get())
                        .peek(t -> ((DirectDB) db).getInto(key0, t.getA().clear()))
                        .peek(t -> ((DirectDB) db).getInto(key1, t.getB().clear()))
                        .peek(t -> this.checkIdentical(arr0, t.getA()))
                        .forEach(t -> this.checkIdentical(arr1, t.getB()));

                System.out.println("getZeroCopy");
                IntStream.range(0, 10000).parallel()
                        .mapToObj(i -> new Tuple<>(((DirectDB) db).getZeroCopy(key0), ((DirectDB) db).getZeroCopy(key1)))
                        .peek(t -> this.checkIdentical(arr0, t.getA()))
                        .peek(t -> this.checkIdentical(arr1, t.getB()))
                        .forEach(t -> {
                            t.getA().release();
                            t.getB().release();
                        });
            }
        }, CompressionType.NONE);
    }

    private void doTest(@NonNull IOConsumer<DB> code, @NonNull CompressionType compression) throws IOException {
        System.out.printf("Opening DB with compression: %s...\n", compression);
        try (DB db = LevelDB.PROVIDER.open(TEST_ROOT, new Options().compressionType(compression))) {
            System.out.println("Opened DB!");

            code.acceptThrowing(db);

            System.out.println("Closing DB...");
        }
        System.out.println("Closed DB!");
    }

    private void checkIdentical(@NonNull byte[] arr, @NonNull ByteBuf buf) {
        for (int i = 0; i < arr.length; i++) {
            checkState(arr[i] == buf.getByte(i), i);
        }
    }
}
