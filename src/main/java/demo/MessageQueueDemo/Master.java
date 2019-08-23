package demo.MessageQueueDemo;

import org.zeromq.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class Master implements Runnable {

    private final static int HEARTBEAT_INTERVAL = 1000;
    //  Paranoid Pirate Protocol constants
    private final static String PPP_READY = "\001"; //  Signals worker is ready
    private final static String PPP_HEARTBEAT = "\002"; //  Signals worker heartbeat
    private ZContext context = null;
    private ArrayList<Worker> workers = new ArrayList<Worker>();
    // int task = 100;
    HashSet<Long> task = new HashSet<Long>();

    Master(ZContext context) {
        this.context = context;

        for (Long num = 100000L; num > 0; num--) {
            task.add(num);
        }
    }

    private static class Worker {
        ZFrame address;  //  Address of worker
        String identity; //  Printable identity
        long expiry;   //  Expires at this time

        protected Worker(ZFrame address) {
            this.address = address;
            identity = new String(address.getData(), ZMQ.CHARSET);

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
        public static ZFrame next(ArrayList<Worker> workers) {
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


    @Override
    public void run() {
        System.out.println("proxy is start:");
        try {
            ZMQ.Socket frontend = context.createSocket(SocketType.ROUTER);
            ZMQ.Socket backend = context.createSocket(SocketType.ROUTER);
            frontend.bind("tcp://*:5555"); //  For clients
            backend.bind("tcp://*:5556"); //  For workers
            //  List of available workers
            ArrayList<Worker> workers = new ArrayList<Worker>();

            //  Send out heartbeats at regular intervals


            ZMQ.Poller poller = context.createPoller(2);
            poller.register(backend, ZMQ.Poller.POLLIN);
            poller.register(frontend, ZMQ.Poller.POLLIN);

            while (true) {
                //boolean workersAvailable = workers.size() > 0;
                int rc = poller.poll(HEARTBEAT_INTERVAL);
                if (rc == -1)
                    break; //  Interrupted

                //  Handle worker activity on backend
                if (poller.pollin(0)) {
                    //  Use worker address for LRU routing
                    ZMsg msg = ZMsg.recvMsg(backend);
                    if (msg == null)
                        break; //  Interrupted

                    //  Any sign of life from worker means it's ready
                    ZFrame address = msg.unwrap();
                    Worker worker = new Worker(address);
                    worker.ready(workers);

                    //  Validate control message, or return reply to client
                    if (msg.size() == 1) {
                        ZFrame frame = msg.getFirst();
                        //判断是否为心跳信息
                        String data = new String(frame.getData(), ZMQ.CHARSET);
                        if (!data.equals(PPP_READY) && !data.equals(PPP_HEARTBEAT)) {
                            System.out.println("E: invalid message from worker");
                            msg.dump(System.out);
                        }
                        msg.destroy();
                    }
                    //解包后size为2 约定是计算结果消息
                    if (msg.size() == 2) {
                        String id = new String(msg.pop().getData(), ZMQ.CHARSET);
                        String value = new String(msg.pop().getData(), ZMQ.CHARSET);

                    }
                    //如果任务未发送完毕，且有可用worker 则发送任务
                    Iterator<Long> iterator = task.iterator();

                    if (workers.size() > 0 && iterator.hasNext()) {
Long task

                        ZMsg sendTaskMsg = new ZMsg();
                        sendTaskMsg.add(Worker.next(workers));
                        sendTaskMsg.add("" + );
                        sendTaskMsg.add("" + "任务" + task);
                        System.out.println("发送任务" + task + "address:" + sendTaskMsg.getFirst());
                        sendTaskMsg.send(backend);
                    }
                }
            }
        } catch (Exception e) {

        }
    }

    public static void main(String[] args) {
        ZContext context = new ZContext();
        Thread thread1 = new Thread(new Master(context));
        thread1.start();
    }
}
