package com.github.ramiyer.nicety;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
        writeLock.lock();
        try {
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
        readLock.lock();
        try {
            V value = null;
            if (concurrentHashMap.contains(key)) {
                concurrentLinkedQueue.remove(key);
                value = concurrentHashMap.get(key);
                concurrentLinkedQueue.add(key);
            }
            return value;
        } finally {
            readLock.unlock();
        }
    }
}
