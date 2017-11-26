package com.wesley.bloblib;

public class RetryObj {
	//等待下一次循环线程睡眠的时间,单位是毫秒
    private int timeToSleep;
    //需要尝试的次数
    private int numberOfRetries;
    //还剩余的次数
    private int numberOfTriesLeft;
    //是否是第一次
    private boolean isFirst;
    //是否已经记录日志
    public boolean hasLogged;
    public RetryObj(int xNumberOfRetries)
    {
        timeToSleep = 200;
        numberOfRetries = xNumberOfRetries;
        numberOfTriesLeft = numberOfRetries;
        isFirst = true;
        hasLogged = false;

    }
    //如要调用函数采用try-catch结构来处理抛出的异常
    //判断是否应该继续尝试
    public boolean shouldRetry()
    {
        //如果尝试次数已到,则返回,或抛出异常
        if(isReachMaxRetryTimes()){return false;};
        //剩余次数减1
        numberOfTriesLeft--;
        //在第一次进入循环,不需要线程休眠
        if (isFirst) { 
            isFirst = false;
            return true;
        }
        //线程休眠,等待下一次尝试
        waitUntilNextTry();
        //返回true,进行下一次尝试
        return true;

    }
    public void errorOccured() throws BfsException
    {
        //但在尝试过程中发生异常时,检查是否已经达到设定的尝试次数
        if (isReachMaxRetryTimes()){
        	throw new BfsException("Retry Failed: Total " + numberOfRetries + " attempts were made.");
        }
        waitUntilNextTry();
    }
    //判断是否应该停止循环,并抛出异常
    public boolean isReachMaxRetryTimes() {
        if (numberOfTriesLeft == 0)
        {
            return true;
        }
        return false;
    }
    
    public void waitUntilNextTry()
    {
        try
        {
            Thread.sleep(timeToSleep);
        }
        catch (Exception ex)
        {
            //这里需要处理线程被中断的异常
        	
        }

    }

}
