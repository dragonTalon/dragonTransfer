package com.dragon.transfer.core.handle.transport;

import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.enums.RecordType;
import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.common.exception.code.ChannelErrorCode;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Title:本地传输
 * @Author dragon
 * @Description
 * @Date 2023/11/30 10:27
 **/
public class LocalTransport {

    private static final Integer DEFAULT_TIMEOUT = 60 * 5;
    /**
     * 库名
     */
    private String db;

    /**
     * 消费队列
     */
    private BlockingQueue<Record> queue;

    public LocalTransport(String db) {
        this(db, 1000);
    }

    public LocalTransport(String db, int size) {
        this.db = db;
        this.queue = new ArrayBlockingQueue<>(size);
        ;
    }


    public void push(Record record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            try {
                TimeUnit.SECONDS.sleep(new Random().nextInt(10));
            } catch (InterruptedException ex) {

            }
            throw DragonTException.asException(ChannelErrorCode.CHANNEL_ERROR_STORE, "记录存储失败");
        }
    }

    public Record get() {
        try {
            Record take = queue.take();
            return take.getType() == RecordType.END ? null : take;
        } catch (InterruptedException e) {
            throw DragonTException.asException(ChannelErrorCode.CHANNEL_GET_FAIL, "获取数据失败");
        }
    }

    public Record get(Long timeout) {
        try {
            return queue.poll(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw DragonTException.asException(ChannelErrorCode.CHANNEL_GET_FAIL, "获取数据失败");
        }
    }


}
