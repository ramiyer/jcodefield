package com.github.ramiyer.nicety;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author ram
 */
public class LRUCache<K,V>
{

    private final int capacity;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = readWriteLock.readLock();
    private Lock writeLock = readWriteLock.writeLock();
    private ConcurrentLinkedQueue<K> concurrentLinkedQueue = new ConcurrentLinkedQueue<K>();
    private ConcurrentHashMap<K,V> concurrentHashMap = new ConcurrentHashMap<K, V>();

    public LRUCache(int capacity ) {
        this.capacity = capacity;
    }

    public void add(K key, V value) {

        try {
            writeLock.lock();
            if(concurrentHashMap.contains(key)){
                concurrentLinkedQueue.remove(key);
            }

            while (concurrentLinkedQueue.size() >= capacity) {
                K oldestKey = concurrentLinkedQueue.poll();
                if (null != oldestKey) {
                    concurrentHashMap.remove(oldestKey);
                }
            }
            concurrentLinkedQueue.add(key);
            concurrentHashMap.put(key,value);
        } finally {
            writeLock.unlock();
        }
    }

    public V get(K key) {
        return concurrentHashMap.get(key);
    }
}
