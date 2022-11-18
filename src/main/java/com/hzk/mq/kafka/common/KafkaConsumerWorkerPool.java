package com.hzk.mq.kafka.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerWorkerPool {

    private static int THREAD_SIZE = Integer.parseInt(System.getProperty("kafka.consumer.worker.pool.size", "64"));

    private static final Integer MAX_FIX_QUEUESIZE = Integer.getInteger("threadpool.fix.maxqueue.size", 10000);

    private static ExecutorService WORKER_POOL = new ThreadPoolExecutor(THREAD_SIZE, THREAD_SIZE, 60L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(MAX_FIX_QUEUESIZE), new ThreadFactory() {
        private AtomicInteger atomicInteger = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            return new Thread(r,  "bosKafkaWorker-" + this.atomicInteger.incrementAndGet());
        }
    }, new ThreadPoolExecutor.DiscardOldestPolicy());

    /**
     * 线程池执行
     * @param runnable runnable
     */
    public static void execute(Runnable runnable){
        WORKER_POOL.execute(runnable);
    }

}
