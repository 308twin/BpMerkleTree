/*
 * Copyright (c) 2006 and onwards Makoto Yui
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package btree4j;

import btree4j.indexer.BasicIndexQuery.IndexConditionBW;
import btree4j.utils.io.FileUtils;
import btree4j.utils.lang.PrintUtils;

import java.io.File;
import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class BTreeTest {
    private static final boolean DEBUG = true;

    @Test
    public void test() throws BTreeException {
        File tmpDir = FileUtils.getFileDir();
        Assert.assertTrue(tmpDir.exists());
        File tmpFile = new File(tmpDir, "BTreeTest1.idx");
        tmpFile.deleteOnExit();
        if (tmpFile.exists()) {
            Assert.assertTrue(tmpFile.delete());
        }

        BTree btree = new BTree(tmpFile);
        btree.init(/* bulkload */ true);
        System.out.println(btree.getRootMerkleHash());

        for (int i = 100; i <= 500; i++) {
            Value k = new Value("k" + i);
            long v = 2;
            btree.addValue(k, v);
            System.out.println(btree.getRootMerkleHash());

        }
        // btree.flush();

        System.out.println(btree.getRootMerkleHash());
        //btree.visualizeBTree();

        // for (int i = 0; i < 1000; i++) {
        // Value k = new Value("k" + i);
        // long expected = i;
        // long actual = btree.findValue(k);
        // Assert.assertEquals(expected, actual);
        // }
        //
        // btree.search(new IndexConditionBW(new Value("k" + 900), new Value("k" +
        // 910)),
        // new BTreeCallback() {
        //
        // @Override
        // public boolean indexInfo(Value value, long pointer) {
        // //System.out.println(pointer);
        // return true;
        // }
        //
        // @Override
        // public boolean indexInfo(Value key, byte[] value) {
        // throw new UnsupportedOperationException();
        // }
        // });
    }

    @Test
    public void testDiffInsert() throws BTreeException {
        File tmpDir = FileUtils.getFileDir();
        Assert.assertTrue(tmpDir.exists());
        File tmpFile = new File(tmpDir, "BTreeTest1.idx");
        tmpFile.deleteOnExit();
        if (tmpFile.exists()) {
            Assert.assertTrue(tmpFile.delete());
        }

        BTree btree = new BTree(tmpFile);
        btree.init(/* bulkload */ true);
       
        for (int i = 999; i >= 100; i--) {
            Value k = new Value("k" + i);
            System.out.println(k);
            long v = i;
            btree.addValue(k, v);
            System.out.println(btree.getRootMerkleHash());
    
        }
        btree.visualizeBTree();


    }

    @Test
    public void testDiffInsert2() throws BTreeException {
        File tmpDir = FileUtils.getFileDir();
        Assert.assertTrue(tmpDir.exists());
        File tmpFile = new File(tmpDir, "BTreeTest1.idx");
        tmpFile.deleteOnExit();
        if (tmpFile.exists()) {
            Assert.assertTrue(tmpFile.delete());
        }

        BTree btree = new BTree(tmpFile);
        btree.init(/* bulkload */ true);
       
        for (int i = 100; i <= 999; i++) {
            Value k = new Value("k" + i);
            System.out.println(k);
            long v = i;
            btree.addValue(k, v);
            System.out.println(btree.getRootMerkleHash());
    
            }

    }

    @Test
    public void testRandomInsert() throws BTreeException {
        File tmpDir = FileUtils.getFileDir();
        Assert.assertTrue(tmpDir.exists());
        File tmpFile = new File(tmpDir, "BTreeTest1.idx");
        tmpFile.deleteOnExit();
        if (tmpFile.exists()) {
            Assert.assertTrue(tmpFile.delete());
        }

        BTree btree = new BTree(tmpFile);
        btree.init(/* bulkload */ true);
        Random random = new Random();
        Set<Integer> generatedNumbers = new HashSet<>();
        
        // 循环直到生成完所有数字
        while (generatedNumbers.size() < 999) {
            int num = random.nextInt(999) + 1;  // 生成1到999之间的随机数
            if (!generatedNumbers.contains(num)) {
                generatedNumbers.add(num);
                Value k = new Value("k" + num);
            btree.addValue(k, num);
            }
            
        }

        btree.visualizeBTree();

    }

    @Test
    public void test10m() throws BTreeException {
        File tmpDir = FileUtils.getTempDir();
        Assert.assertTrue(tmpDir.exists());
        File indexFile = new File(tmpDir, "test10m.idx");
        indexFile.deleteOnExit();
        if (indexFile.exists()) {
            Assert.assertTrue(indexFile.delete());
        }

        BTree btree = new BTree(indexFile, false);
        btree.init(false);

        final Map<Value, Long> kv = new HashMap<>();
        final Random rand = new Random();
        for (int i = 0; i < 1000000; i++) {
            long nt = System.nanoTime(), val = rand.nextInt(Integer.MAX_VALUE); // FIXME val = rand.nextLong();
            Value key = new Value(String.valueOf(nt) + val);
            btree.addValue(key, val);
            if (i % 10000 == 0) {
                kv.put(key, val);
                // println("put k: " +z key + ", v: " + val);
            }
            Assert.assertEquals(val, btree.findValue(key));

            // if (i % 1000000 == 0) {
            // btree.flush();
            // }
        }
        btree.flush(true, true);
        btree.close();

        Assert.assertTrue(indexFile.exists());
        println("File size of '" + FileUtils.getFileName(indexFile) + "': "
                + PrintUtils.prettyFileSize(indexFile));

        btree = new BTree(indexFile, false);
        btree.init(false);
        for (Entry<Value, Long> e : kv.entrySet()) {
            Value k = e.getKey();
            Long v = e.getValue();
            long result = btree.findValue(k);
            Assert.assertNotEquals("key is not registered: " + k, BTree.KEY_NOT_FOUND, result);
            Assert.assertEquals("Exexpected value '" + result + "' found for key: " + k,
                    v.longValue(), result);
        }
    }

    private static void println(String msg) {
        if (DEBUG) {
            System.out.println(msg);
        }
    }

}
