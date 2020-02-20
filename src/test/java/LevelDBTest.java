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

import net.daporkchop.ldbjni.LevelDB;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.encoding.ToBytes;
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

        try (DB db = LevelDB.PROVIDER.open(TEST_ROOT, new Options())) {
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
