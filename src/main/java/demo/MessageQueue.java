package demo;

import demo.Client_Side_Reliability.ppqueue;
import org.zeromq.*;

import java.util.ArrayList;
import java.util.Iterator;

public class MessageQueue{
    private final static int HEARTBEAT_LIVENESS = 3;    //  3-5 is reasonable
    private final static int HEARTBEAT_INTERVAL = 1000; //  msecs
    //  Paranoid Pirate Protocol constants
    private final static String PPP_READY = "\001"; //  Signals worker is ready
    private final static String PPP_HEARTBEAT = "\002"; //  Signals worker heartbeat
    private final static String WORK = "\003";    //任务标记
    private final static String RESULT = "\004";  //结果标记

    private ArrayList<Integer> result = new ArrayList<Integer>();    //结果集


    //  Here we define the worker class; a structure and a set of functions that
    //  as constructor, destructor, and methods on worker objects:
    private static class Worker {
        ZFrame address;  //  Address of worker
        String identity; //  Printable identity
        long expiry;   //  Expires at this time

        protected Worker(ZFrame address) {
            this.address = address;
            identity = new String(address.getData(), ZMQ.CHARSET);
            expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
        }

        //  The ready method puts a worker to the end of the ready list:
        protected void ready(ArrayList<Worker> workers) {
            Iterator<Worker> it = workers.iterator();
            while (it.hasNext()) {
                Worker worker = it.next();
                if (identity.equals(worker.identity)) {
                    it.remove();
                    break;
                }
            }
            workers.add(this);
        }

        //  The next method returns the next available worker address:
        protected static ZFrame next(ArrayList<Worker> workers) {
            Worker worker = workers.remove(0);
            assert (worker != null);
            ZFrame frame = worker.address;
            return frame;
        }

        //  The purge method looks for and kills expired workers. We hold workers
        //  from oldest to most recent, so we stop at the first alive worker:
        protected static void purge(ArrayList<Worker> workers) {
            Iterator<Worker> it = workers.iterator();
            while (it.hasNext()) {
                Worker worker = it.next();
                if (System.currentTimeMillis() < worker.expiry) {
                    break;
                }
                it.remove();
            }
        }
    }

    public static void main(String[] args) {
        try {
            ZContext context = new ZContext();
            ZMQ.Socket vent = context.createSocket(SocketType.ROUTER);
            vent.bind("tcp://*:5555");
            //创建worker队列
            ArrayList<Worker> workers = new ArrayList<Worker>();

            //创建心跳
            long heartbeat_at = System.currentTimeMillis() + HEARTBEAT_INTERVAL;

            //创建Poller
            ZMQ.Poller poller = context.createPoller(1);
            poller.register(vent, ZMQ.Poller.POLLIN);

            //检测 worker状态， 消息
            while (true) {
                boolean workersAvailable = workers.size() > 0;

                int rc = poller.poll(HEARTBEAT_INTERVAL);
                if (rc == -1) {
                    break; //  Interrupted
                }
                //  处理worker消息: 1.心跳 2.ready 3.其他
                if (poller.pollin(0)) {
                    //  Use worker address for LRU routing
                    ZMsg msg = ZMsg.recvMsg(vent);
                    if (msg == null) {
                        break; //  Interrupted
                    }

                    //根据消息头判断消息类型
                    ZFrame frame = msg.getFirst();
                    String data = new String(frame.getData(), ZMQ.CHARSET);

                    if(msg.size() == 1){
                        if (data.equals(PPP_READY) || data.equals(PPP_HEARTBEAT)) {
                            System.out.println("========>update queue<========");

                            //根据消息头中的地址创建worker 加入队列,如果队列中已存在，则删除，重新加入，（改为更新心跳）
                            ZFrame address = msg.unwrap();
                            Worker worker = new Worker(address);
                            worker.ready(workers);
                        }else{
                            System.out.println("========>invalid message from worker<========");
                        }

                    }
                    if (msg.size() == 3 && RESULT.equals(data)){
                        //todo  get content
                        System.out.println("========>get result from worker<========");
                    }

                    msg.destroy();
                }

            }
        } catch (Exception e) {

        }
    }
}
