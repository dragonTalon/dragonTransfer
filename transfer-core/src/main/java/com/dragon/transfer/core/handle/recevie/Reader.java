package com.dragon.transfer.core.handle.recevie;

import java.util.concurrent.Callable;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/30 17:14
 **/
public abstract class Reader {

    public abstract void startReader();


    public static abstract class Job implements Callable<Boolean> {
        @Override
        public Boolean call() {
            preCheck();
            preHandle();
            read();
            afterHandle();
            return true;
        }

        protected abstract void preCheck();

        protected abstract void preHandle();

        protected abstract void afterHandle();


        protected abstract void read();
    }
}
