package com.github.ramiyer.leveldb;

import com.github.ramiyer.Store;

/**
 * @author ram
 */
public interface KeyValueDataStore<K,V> extends Store
{

    public void put(final K key,final V value) throws DataStoreException;

    public V get(final K key);

}
