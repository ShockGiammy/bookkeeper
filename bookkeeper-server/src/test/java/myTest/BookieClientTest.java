package myTest;

import static java.nio.charset.StandardCharsets.UTF_8;
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

    byte[] passwd;
    ByteBufList byteBuf;
    long flags;
    static int init_rc;

    @Before
    public void configure() throws Exception {
    	
    	//server configuation
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

        //server start
        bs = new BookieServer(conf);
        bs.start();
        
        
        //client configuration
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
        client = new BookieClientImpl(new ClientConfiguration(), eventLoopGroup,
                UnpooledByteBufAllocator.DEFAULT, executor, scheduler, NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        
        addr = bs.getBookieId();
        result = new ResultStruct();
        
        flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE
                | BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        byteBuf = createByteBuffer(1, 1, 1);
        init_rc = -1000;			//some negative values are used for custom exceptions
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
        int rc = init_rc;
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
    

    private ByteBufList createByteBuffer(int i, long lid, long eid) {
        ByteBuf byteBuf = Unpooled.buffer(4 + 24);
        byteBuf.writeLong(lid);
        byteBuf.writeLong(eid);
        byteBuf.writeLong(eid - 1);
        byteBuf.writeInt(i);
        return ByteBufList.get(byteBuf);
    }
    
    @Test
    public void testCorrectReadAndWrite() {
    
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
    }
    
    @Test
    public void testNegativeEntryReadAndWrite() {
        
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, -3, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        
        result.rc = init_rc;
        synchronized (result) {
            client.readEntry(addr, 1, -3, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);			//result not written
        }
    }
    
    @Test
    public void testZeroEntryReadAndWrite() {
        
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 0, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        
        result.rc = init_rc;
        synchronized (result) {
            client.readEntry(addr, 1, 0, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);		//result written but entryId not valid
        }
    }
    
    @Test
    public void testZeroLedgerReadAndWrite() {
        
        synchronized (result) {
        	client.addEntry(addr, 0, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 0, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    @Test
    public void testNegativeLedgerReadAndWrite() {
        
        synchronized (result) {
        	client.addEntry(addr, -3, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, -3, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    @Test
    public void testTrueFastFail() {
        
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, true, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
    }
    
    @Test
    public void testWrongEntry() {
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 2, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(BKException.Code.NoSuchEntryException, result.rc);
    	}
    }
    
    @Test
    public void testnullPassword() {
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, null, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
    	}
    }
    
    @Test
    public void testnullWriteCallback() {
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, null, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);				// no one writes on result
            												//no entry written on ledger
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(1, result.entry.getInt());
    	}
    }
    
    @Test 
    public void testnullReadCallback() {
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	result.rc = init_rc;
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, null, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(init_rc, result.rc);				//ledger written but no recb = no write on result
    	}
    }
    
    @Test
    public void testnullResult() {
    	
    	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(init_rc, result.rc);			//result as initial
    	
    	client.readEntry(addr, 1, 1, recb, null, BookieProtocol.FLAG_NONE);
    	assertEquals(init_rc, result.rc);
    }
    
    @Test
    public void testDeferred_Sync() {
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, EnumSet.of(WriteFlag.DEFERRED_SYNC));
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(1, result.entry.getInt());
    	}
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullBuffer() {
    	
    	client.addEntry(addr, 1, passwd, 1, null, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    }
    
    @Test
    public void testHelloValue() {
    	
    	byte[] hello = "hello".getBytes(UTF_8);
        byteBuf = ByteBufList.get(Unpooled.wrappedBuffer(hello));
        
        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.WriteException, result.rc);
        }
        /*}
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }*/
    }
    
    @Test
    public void testNotValidBufferValues() {
    	
    	ByteBuf byteBufWrong = Unpooled.buffer(4 + 24);
    	byteBufWrong.writeLong(-1);
    	byteBufWrong.writeLong(-1);
    	byteBufWrong.writeLong(-2);
    	byteBufWrong.writeInt(-1);
        byteBuf = ByteBufList.get(byteBufWrong);
        
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);		//ledger not written
    	}
    }
    
    @Test
    public void testNotValidBufferOverSize() {
    	
    	ByteBuf byteBufWrong = Unpooled.buffer(36);
    	byteBufWrong.writeLong(1);
    	byteBufWrong.writeLong(1);
    	byteBufWrong.writeLong(0);
    	byteBufWrong.writeInt(1);
        byteBuf = ByteBufList.get(byteBufWrong);
        
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(1, result.entry.getInt());
    	}
    }
    
    @Test
    public void testNotValidBufferUnderSize() {
    	
    	ByteBuf byteBufWrong = Unpooled.buffer(8);
    	byteBufWrong.writeLong(1);
    	byteBufWrong.writeLong(1);
    	byteBufWrong.writeLong(0);
    	byteBufWrong.writeInt(1);
        byteBuf = ByteBufList.get(byteBufWrong);
        
    	
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(1, result.entry.getInt());
    	}
    }
    
    @Test
    public void testMultipleWrite() {
    	
    
    	byteBuf = createByteBuffer(2, 1, 2);
    	client.addEntry(addr, 1, passwd, 2, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    	byteBuf = createByteBuffer(3, 1, 3);
    	client.addEntry(addr, 1, passwd, 3, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    	byteBuf = createByteBuffer(5, 1, 5);
    	client.addEntry(addr, 1, passwd, 5, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    	byteBuf = createByteBuffer(7, 1, 7);
    	client.addEntry(addr, 1, passwd, 7, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    	
    	synchronized (result) {
    		client.readEntry(addr, 1, 7, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(7, result.entry.getInt());
    	}
    }
    
    @Test
    public void testNoLedger() {
        
        synchronized (result) {
            client.readEntry(addr, 2, 13, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    
    @Test
    public void testClientWrong() {		//to reach Branch Coverage
        
        BookieId wrongAddr = BookieId.parse("127.0.0.2");
        
        synchronized (result) {
        	client.addEntry(wrongAddr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
        synchronized (result) {
            client.readEntry(wrongAddr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
    }
    
    @Test
    public void testClientClose() {		//to reach Branch Coverage
    	
    	client.close();					//closed = true
    	assertTrue(client.isClosed());
    	
    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);

        synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized(result) {
        	client.forceLedger(addr, 1, flcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
            client.writeLac(addr, 1, passwd, 5, byteBuf, wrlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, 1, 1, 0, 0, false, recb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
        
        CallbackObj obj = new CallbackObj(flags);
        client.getBookieInfo(addr, flags, gbicb, obj);
        try {
			obj.latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        assertEquals(BKException.Code.ClientClosedException, obj.rc);
        assertEquals(0, obj.freeDiskSpace);
        assertEquals(0, obj.totalDiskCapacity);
    }
    
    @Test
    public void testFaultyBookies() {
    	
    	client.lookupClient(addr);
    	
    	List<BookieId> faultyBookies = client.getFaultyBookies();
    	assertEquals(0, faultyBookies.size());
    }
    
    @Test
    public void testForceLedger() {

    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);
    	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(-1000, result.rc);
    	
        synchronized(result) {
        	client.forceLedger(addr, 1, flcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	assertEquals(0, result.rc);
        }
    }
    
    @Test
    public void testGetListOfEntriesOfLedger() {
    	ByteBufList byteBuf = createByteBuffer(1, 1, 1);
    	long ledgerId = 1;

        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, EnumSet.of(WriteFlag.DEFERRED_SYNC));
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	assertNotNull(client.getListOfEntriesOfLedger(addr, 1));
    	
    	client.close();
    	assertNotNull(client.getListOfEntriesOfLedger(addr, 1));
    }
    
    @Test
    public void testPendingRequest() {

    	assertEquals(0, client.getNumPendingRequests(addr, 1));

    	//ByteBufList byteBuf = createByteBuffer(1, 1, 1);
        
    	synchronized (result) {
        	client.addEntry(addr, 1, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	synchronized (result) {
            client.readEntry(addr, 1, 1, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(1, result.entry.getInt());
        }
    	assertEquals(0, client.getNumPendingRequests(addr, 1));
    	
    	client.close();
    	assertEquals(0, client.getNumPendingRequests(addr, 1));
    } 
    
    @Test
    public void ReadAndWriteLAC() {
    	
    	byteBuf = createByteBuffer(16, 1, 16);
          
        synchronized (result) {
            client.readLac(addr, 1, rlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);
        }
        
        synchronized (result) {
        	client.writeLac(addr, 1, passwd, 9, byteBuf, wrlcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }   

        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, 1, 9, 16, 1000, true, recb, result);
        	assertEquals(0, result.rc);
        }
    }

    @Test
    public void testGetBookieInfo() {

        CallbackObj obj = new CallbackObj(flags);
        
        client.getBookieInfo(addr, flags, gbicb, obj);
        try {
			obj.latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        System.out.println("Return code: " + obj.rc + "FreeDiskSpace: " + obj.freeDiskSpace + " TotalCapacity: "
                + obj.totalDiskCapacity);
        
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.rc == Code.OK);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.freeDiskSpace <= obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.totalDiskCapacity > 0);
    }
}
