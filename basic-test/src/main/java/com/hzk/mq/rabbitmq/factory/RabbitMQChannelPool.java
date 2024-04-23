package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * rabbitmq生产者的channel池，publisher与channel解耦
 */
public class RabbitMQChannelPool extends BasePooledObjectFactory<Channel> {

    private static Connection connection;
    static {
        try {
            connection = RabbitMQFactory.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private GenericObjectPool<Channel> objectPool;

    public void setObjectPool(GenericObjectPool<Channel> objectPool) {
        this.objectPool = objectPool;
    }

    @Override
    public Channel create() throws Exception {
        return connection.createChannel();
    }

    @Override
    public PooledObject<Channel> wrap(Channel obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public boolean validateObject(PooledObject<Channel> p) {
        return p.getObject().isOpen();
    }

    /**
     * 归还对象时的判定maxIdle > idleObjects，销毁归还对象
     * 优化点：可提高maxIdle满足高并发时所需channel数
     * @param pooledObject pooledObject
     */
    @Override
    public void destroyObject(PooledObject<Channel> pooledObject) {
        try {
            pooledObject.getObject().close();
            // 提高maxIdle
            objectPool.setMaxIdle((int)(Math.min(objectPool.getMaxIdle() * 1.2, objectPool.getMaxTotal())));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
