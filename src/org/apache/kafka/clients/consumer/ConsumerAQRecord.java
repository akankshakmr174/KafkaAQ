package org.apache.kafka.clients.consumer;

public class ConsumerAQRecord<K,V>{
    K key;
    V val;
    public ConsumerAQRecord(){
        key=null;
        val=null;
    }
    public K key() {
        return key;
    }
    public V value() {
        return val;
    }
    public ConsumerAQRecord<K,V> retVal(){
        ConsumerAQRecord<K,V> record=null;
        record.key=key();
        record.val=value();
        return record;
    }
}
