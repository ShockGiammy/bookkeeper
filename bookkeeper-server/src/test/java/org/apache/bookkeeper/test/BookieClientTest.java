package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the bookie client.
 */
public class BookieClientTest {
	
    BookieServer bs;
    File tmpDir;
    public int port;
    ServerConfiguration conf;

    public EventLoopGroup eventLoopGroup;
    public OrderedExecutor executor;
    private ScheduledExecutorService scheduler;
    private BookieClientImpl client;
    BookieId addr;
    ResultStruct result;
    String[] mainArgs;
    byte[] passwd;
    ByteBufList byteBuf;
    long flags;

    @Before
    public void configure() throws Exception {
    	
    	port = 13645;
    	
        tmpDir = IOUtils.createTempDir("bookieClient", "test");

        conf = new ServerConfiguration();
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setJournalFormatVersionToWrite(5);
        conf.setAllowEphemeralPorts(true);
        conf.setBookiePort(0);
        conf.setGcWaitTime(1000);
        conf.setDiskUsageThreshold(0.999f);
        conf.setDiskUsageWarnThreshold(0.99f);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        conf.setListeningInterface(getLoopbackInterfaceName());
        conf.setAllowLoopback(true);

        conf.setBookiePort(port).setJournalDirName(tmpDir.getPath()).setLedgerDirNames(new String[] { tmpDir.getPath() }).setMetadataServiceUri(null);

        bs = new BookieServer(conf);
        bs.start();
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
        
        addr = bs.getBookieId();
        result = new ResultStruct();
        
        flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE
                | BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        client = new BookieClientImpl(new ClientConfiguration(), eventLoopGroup,
                UnpooledByteBufAllocator.DEFAULT, executor, scheduler, NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        
    }
    

    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
        	System.out.println(se.getMessage());
            return null;
        }
        System.out.println("Unable to deduce loopback interface.");
        return null;
    }

    @After
    public void tearDown() throws Exception {
        scheduler.shutdown();
        bs.shutdown();
        recursiveDelete(tmpDir);
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
        client.close();
    }

    private static void recursiveDelete(File dir) {
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                recursiveDelete(child);
            }
        }
        dir.delete();
    }

    static class ResultStruct {
        int rc = -1000;			//some negative values are used for custom exceptions
        ByteBuffer entry;
    }

    ReadEntryCallback recb = new ReadEntryCallback() {

        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && buffer != null) {
                	buffer.readerIndex(24);
                    rs.entry = buffer.nioBuffer();
                }
                rs.notifyAll();
            }
        }
    };

    WriteCallback wrcb = new WriteCallback() {
    	
        public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
        }
    };
    
    WriteLacCallback wrlcb = new WriteLacCallback() {
    	
		@Override
		public void writeLacComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
			if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
			
		}
    };
        
    ReadLacCallback rlcb = new ReadLacCallback() {

		@Override
		public void readLacComplete(int rc, long ledgerId, ByteBuf lac, ByteBuf buffer, Object ctx) {
			ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && buffer != null) {
                	buffer.readerIndex(24);
                    rs.entry = buffer.nioBuffer();
                }
                rs.notifyAll();
            }
        }
    };
    
    ForceLedgerCallback flcb = new ForceLedgerCallback() {

		@Override
		public void forceLedgerComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
			if (ctx != null) {
				synchronized (ctx) {
					if (ctx instanceof ResultStruct) {
						ResultStruct rs = (ResultStruct) ctx;
						rs.rc = rc;
					}
					ctx.notifyAll();
				}
			}
		}
    	
    };
    
    GetBookieInfoCallback gbicb = new GetBookieInfoCallback() {
        @Override
        public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
            CallbackObj obj = (CallbackObj) ctx;
            obj.rc = rc;
            if (rc == Code.OK) {
                if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                    obj.freeDiskSpace = bInfo.getFreeDiskSpace();
                }
                if ((obj.requested
                        & BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
                    obj.totalDiskCapacity = bInfo.getTotalDiskSpace();
                }
            }
            obj.latch.countDown();
        }
    };
    
    @Test
    public void testFaultyBookies() {
    	
    	client.lookupClient(addr);
    	
    	List<BookieId> faultyBookies = client.getFaultyBookies();
    	assertEquals(0, faultyBookies.size());
    }
    
    @Test
    public void testClientWrong() throws InterruptedException {		//to reach Branch Coverage
        ByteBufList byteBuf = createByteBuffer(1, 1, 1);
        BookieId wrongAddr = BookieId.parse("127.0.0.2");
        
        synchronized (result) {
        	client.addEntry(wrongAddr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            result.wait(1000);
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
        synchronized (result) {
            client.readEntry(wrongAddr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
    }
    
    @Test
    public void testClientClose() throws InterruptedException {		//to reach Branch Coverage
    	
    	client.close();					//closed = true
    	assertTrue(client.isClosed());
    	
    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);

        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            result.wait(1000);
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized(result) {
        	client.forceLedger(addr, 1, flcb, result);
        	result.wait(1000);
        	assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
            client.writeLac(addr, 1, passwd, 5, byteBuf, wrlcb, result);
            result.wait(1000);
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            result.wait(1000);
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, 1, 1, 0, 0, false, recb, result);
        	result.wait(1000);
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
        
        CallbackObj obj = new CallbackObj(flags);
        client.getBookieInfo(addr, flags, gbicb, obj);
        obj.latch.await();
        assertEquals(BKException.Code.ClientClosedException, obj.rc);
        assertEquals(0, obj.freeDiskSpace);
        assertEquals(0, obj.totalDiskCapacity);
    }
    
    @Test
    public void testForceLedger() throws InterruptedException {

    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);
    	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(-1000, result.rc);
    	
        synchronized(result) {
        	client.forceLedger(addr, 1, flcb, result);
        	result.wait(1000);
        	assertEquals(0, result.rc);
        }
    }
    
    @Test
    public void testGetListOfEntriesOfLedger() throws InterruptedException {
    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);
    	long ledgerId = 1;

        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, EnumSet.of(WriteFlag.DEFERRED_SYNC));
            result.wait(1000);
            assertEquals(0, result.rc);
        }
    	assertNotNull(client.getListOfEntriesOfLedger(addr, 1));
    	
    	client.close();
    	assertNotNull(client.getListOfEntriesOfLedger(addr, 1));
    }
    
    @Test
    public void testPendingRequest() throws InterruptedException {

    	assertEquals(0, client.getNumPendingRequests(addr, 1));

    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);
        
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
        }
    	synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
    	assertEquals(0, client.getNumPendingRequests(addr, 1));
    	
    	client.close();
    	assertEquals(0, client.getNumPendingRequests(addr, 1));
    }
    
    /*@Test
    public void mainTest() {
    	mainArgs = new String[2];
        mainArgs[0] = addr.toString();
        mainArgs[1] = String.valueOf(port);
    	try {
			BookieClientImpl.main(mainArgs);
		} catch (NumberFormatException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
    	mainArgs = new String[3];
        mainArgs[0] = addr.toString();
        mainArgs[1] = String.valueOf(port);
        mainArgs[2] = "12345";
    	try {
			BookieClientImpl.main(mainArgs);
		} catch (NumberFormatException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
    }*/

    @Test
    public void testReadAndWrite() throws Exception {
    	
        final Object notifyObject = new Object();
        byteBuf = createByteBuffer(1, 1, 1);
        
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
        
        byteBuf = createByteBuffer(2, 1, 2);
        client.addEntry(addr, 1, passwd, 2, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        byteBuf = createByteBuffer(3, 1, 3);
        client.addEntry(addr, 1, passwd, 3, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        byteBuf = createByteBuffer(5, 1, 5);
        client.addEntry(addr, 1, passwd, 5, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        byteBuf = createByteBuffer(7, 1, 7);
        client.addEntry(addr, 1, passwd, 7, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        
        
        synchronized (notifyObject) {
            byteBuf = createByteBuffer(11, 1, 11);
            client.addEntry(addr, 1, passwd, 11, byteBuf, wrcb, notifyObject, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            notifyObject.wait();
        }
        
       
        synchronized (result) {
            client.readEntry(addr, 1, 6, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 7, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(7, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 2, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(2, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 3, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(3, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 4, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 11, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(11, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 5, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(5, result.entry.getInt());
        }
        synchronized (result) {
            client.readEntry(addr, 1, 10, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);
        }
        
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(11, result.entry.getInt());
        }
    }
    
    @Test
    public void ReadAndWriteLAC() throws InterruptedException {
    	
    	byteBuf = createByteBuffer(16, 1, 16);
        
    	client.addEntry(addr, 1, passwd, 16, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);   
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            result.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);
        }
        
        synchronized (result) {
        	client.writeLac(addr, 1, passwd, 9, byteBuf, wrlcb, result);
        	result.wait(1000);
            assertEquals(0, result.rc);
        }   

        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, 1, 9, 16, 1000, true, recb, result);
        	assertEquals(0, result.rc);
        }
        
        synchronized (result) {
            client.writeLac(addr, 1, passwd, 5, byteBuf, wrlcb, result);
            result.wait(1000);
            assertEquals(0, result.rc);
        }
        
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            result.wait(1000);
            assertEquals(0, result.rc);
            assertEquals(16, result.entry.getInt());
        }
    }

    private ByteBufList createByteBuffer(int i, long lid, long eid) {
        ByteBuf byteBuf = Unpooled.buffer(4 + 24);
        byteBuf.writeLong(lid);
        byteBuf.writeLong(eid);
        byteBuf.writeLong(eid - 1);
        byteBuf.writeInt(i);
        return ByteBufList.get(byteBuf);
    }

    @Test
    public void testNoLedger() throws Exception {
        
        synchronized (result) {
            client.readEntry(addr, 2, 13, recb, result, BookieProtocol.FLAG_NONE);
            result.wait(1000);
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }

    @Test
    public void testGetBookieInfo() throws InterruptedException {

        CallbackObj obj = new CallbackObj(flags);
        
        client.getBookieInfo(addr, flags, gbicb, obj);
        obj.latch.await();
        
        System.out.println("Return code: " + obj.rc + "FreeDiskSpace: " + obj.freeDiskSpace + " TotalCapacity: "
                + obj.totalDiskCapacity);
        
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.rc == Code.OK);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.freeDiskSpace <= obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.totalDiskCapacity > 0);
    }
}
