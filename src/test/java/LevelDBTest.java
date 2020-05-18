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

import net.daporkchop.ldbjni.LevelDB;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.encoding.ToBytes;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
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

    static {
        if (PFiles.checkDirectoryExists(TEST_ROOT)) {
            PFiles.rmContents(TEST_ROOT);
        }
    }

    @Test
    public void test() throws IOException {
        if (!LevelDB.PROVIDER.isNative()) {
            throw new IllegalStateException("Not using native LevelDB!");
        }

        try (DB db = LevelDB.PROVIDER.open(TEST_ROOT, new Options().compressionType(CompressionType.SNAPPY))) {
            if (true) {
                int cnt = 100000;
                int batchSize = 1000;
                IntStream.range(0, cnt / batchSize)
                        .forEach(i -> {
                            try (WriteBatch writeBatch = db.createWriteBatch()) {
                                for (int j = 0; j < batchSize; j++) {
                                    byte[] arr = new byte[ThreadLocalRandom.current().nextInt(10, 100000)];
                                    //ThreadLocalRandom.current().nextBytes(arr);
                                    writeBatch.put(ToBytes.toBytes(i * batchSize + j), arr);
                                }

                                db.write(writeBatch);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.println(i);
                        });
            } else if (false)    {
                db.put(ToBytes.toBytes(0), new byte[1 << 20]);

                //get it a bunch of times to see if the byte[]s are actually being GC-d
                IntStream.range(0, 1000000).parallel()
                        .forEach(i -> db.get(ToBytes.toBytes(0)));
            }

            System.out.println(db.get(ToBytes.toBytes(0)).length);
            db.delete(ToBytes.toBytes(0));

            try {
                System.out.println(db.get(ToBytes.toBytes(0)).length);
                throw new IllegalStateException();
            } catch (NullPointerException e) {
                //this should be thrown
            }

            System.out.println("Wrote values!");

            db.compactRange(ToBytes.toBytes(0), ToBytes.toBytes(100));

            System.out.println("Closing DB...");
        }

        System.out.println("Closed DB!");
    }
}
