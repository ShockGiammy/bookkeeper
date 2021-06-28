package myTest;

import java.util.concurrent.CountDownLatch;

class CallbackObj {
    int rc;
    long requested;
    long freeDiskSpace, totalDiskCapacity;
    CountDownLatch latch = new CountDownLatch(1);

    CallbackObj(long requested) {
        this.requested = requested;
        this.rc = 0;
        this.freeDiskSpace = 0L;
        this.totalDiskCapacity = 0L;
    }
}
