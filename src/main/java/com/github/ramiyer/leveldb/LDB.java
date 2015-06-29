package com.github.ramiyer.leveldb;

import com.google.common.collect.Lists;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

public class LDB implements KeyValueDataStore<String,String>
{
    private final DB db;

    public LDB(DB db) {
        this.db = db;
    }

    public List<String> findByPrefix(String prefix, int substringStartsAt) {
        try (DBIterator iterator = db.iterator()) {
            List<String> keys = Lists.newArrayList();
            for (iterator.seek(bytes(prefix)); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                if (key == null)
                    return null;
                if (!key.startsWith(prefix)) {
                    break;
                }
                keys.add(key.substring(substringStartsAt));
            }
            return keys;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String key, String value) {
        db.put(bytes(key), bytes(value));
    }

    public void put(String key, int value) {
        db.put(bytes(key), bytes(String.valueOf(value)));
    }

    public String get(String key) {
        byte[] bytes = db.get(bytes(key));
        //noinspection ReturnOfNull
        return (bytes == null ? null : asString(bytes));
    }

    public void close() throws IOException
    {
        db.close();
    }

    public void put(AtomicWrite atomicWrite) {
        org.iq80.leveldb.WriteBatch origWriteBatch = db.createWriteBatch();
        WriteBatch writeBatch = new WriteBatch(origWriteBatch);
        atomicWrite.write(writeBatch);
        db.write(origWriteBatch);
    }

    public interface AtomicWrite {
        public void write(WriteBatch writeBatch);
    }

    public static class WriteBatch {
        private final org.iq80.leveldb.WriteBatch writeBatch;

        private WriteBatch(org.iq80.leveldb.WriteBatch writeBatch) {
            this.writeBatch = writeBatch;
        }

        public void put(String key, byte[] value) {
            writeBatch.put(bytes(key), value);
        }
    }

    public static void main (String[] args) throws IOException
    {
        Options options = new Options();
        LDB ldb = new LDB(new DbImpl(options, new File(args[0])));
        for (int i = 0 ; i < 10000 ; i ++) {
            if (i % 2 == 0)
                ldb.put("e" +randomSeriesForThreeCharacter()+i, "v_"+i);
            else
                ldb.put("o" +randomSeriesForThreeCharacter()+i, "v_"+i);
        }
        System.out.println(ldb.findByPrefix("o", 0));
    }

    public static char randomSeriesForThreeCharacter() {
        Random r = new Random();
        return (char) (48 + r.nextInt(47));
    }

}
