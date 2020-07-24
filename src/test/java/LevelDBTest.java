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

import lombok.NonNull;
import net.daporkchop.ldbjni.LevelDB;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.encoding.ToBytes;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * @author DaPorkchop_
 */
public class LevelDBTest {
    public static final File TEST_ROOT = new File("test_out");

    @BeforeClass
    public static void ensureNative()  {
        if (!LevelDB.PROVIDER.isNative()) {
            throw new IllegalStateException("Not using native LevelDB!");
        }
    }

    @Before
    public void nukeTestDir()   {
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
    public void testManyReads() throws IOException  {
        this.doTest(db -> {
            //write a single large entry
            db.put(ToBytes.toBytes(0), new byte[1 << 20]);

            //compact it
            db.compactRange(null, null);

            //get it a bunch of times to see if the byte[]s are actually being GC-d
            IntStream.range(0, 10000 * 2).parallel()
                    .peek(i -> {
                        if ((i & 511) == 0)    {
                            System.gc();
                        }
                    })
                    .forEach(i -> db.get(ToBytes.toBytes(0)));
        }, CompressionType.SNAPPY);
    }

    private void doTest(@NonNull IOConsumer<DB> code, @NonNull CompressionType compression) throws IOException  {
        System.out.printf("Opening DB with compression: %s...\n", compression);
        try (DB db = LevelDB.PROVIDER.open(TEST_ROOT, new Options().compressionType(compression)))   {
            System.out.println("Opened DB!");

            code.acceptThrowing(db);

            System.out.println("Closing DB...");
        }
        System.out.println("Closed DB!");
    }
}
