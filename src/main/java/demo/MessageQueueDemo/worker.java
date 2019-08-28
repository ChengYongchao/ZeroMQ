package demo.MessageQueueDemo;

import org.zeromq.*;

import java.io.IOException;
import java.security.Policy;
import java.util.*;

import static org.zeromq.ZMQ.*;


//worker DEALER
public class worker implements Runnable {

    ZContext context = null;

    private final static int HEARTBEAT_INTERVAL = 1000;
    private final static String WORKER_READY = "\001"; //  Signals worker is ready
    private final static String WORKER_RESULT = "\002"; //  worker计算完成返回消息标志
    private final static String MASTER_TASK = "\003";   //主节点发送任务消息标志
    private final static String MASTER_VERTEX = "\004";   //主节点发送顶点消息标志

    private int index;

    private List vertex = null;//顶点集

    public worker(ZContext cxt, int index) {
        this.context = cxt;
        this.index = index;
    }

    @Override
    public void run() {
        try {
            System.out.println("Worker is started:");
            Socket worker = context.createSocket(SocketType.DEALER);
            worker.connect("tcp://*:5556");
            ZMsg readymsg = new ZMsg();
            readymsg.add(new ZFrame(WORKER_READY));
            readymsg.send(worker);

            Poller poller = context.createPoller(1);
            poller.register(worker, Poller.POLLIN);

            while (true) {
                int rc = poller.poll(1000);
                if (rc == -1)
                    break; //  Interrupted

                if (poller.pollin(0)) {
                    ZMsg receivedmsg = ZMsg.recvMsg(worker);
                    if (receivedmsg == null) break;

                    //todo 消息处理机制
                    String signal = new String(receivedmsg.getFirst().getData(), CHARSET);
                    if (MASTER_VERTEX.equals(signal)) {
                        System.out.println("====>接收到顶点信息<======");
                        vertex = (List) Master.serializeToObject(receivedmsg.getLast().getData());

                    } else if (MASTER_TASK.equals(signal)) {

                        try {
                            Map<Integer, Integer> task = (HashMap) Master.serializeToObject(receivedmsg.getLast().getData());
                            Iterator<Integer> id = vertex.iterator();
                            while (id.hasNext()) {
                                Integer vertexId = id.next();
                                Integer resultNum = (Integer) task.get(vertexId);
                                task.put(vertexId, resultNum - 100);
                            }

                            //任务完成,返回消息
                            ZMsg taskFinishedMsg = new ZMsg();
                            taskFinishedMsg.add(WORKER_RESULT);
                            taskFinishedMsg.add(Master.serialize(task));
                            taskFinishedMsg.send(worker);
                            System.out.println("======>任务完成，返回结果<========");
                        } catch (IOException e) {
                            System.err.println("序列化失败");
                        }


                    }

                    receivedmsg.destroy();
                }
            }

        } catch (IOException e) {
            System.err.println("序列化异常");
        } catch (Exception e) {
            System.err.println("异常");
        }
    }

    public static void main(String[] args) {
        ZContext context = new ZContext();
        //Thread thread1 = new Thread(new worker(context));
        //thread1.start();
        for (int num = 2; num > 0; num--) {
            new Thread(new worker(context, num)).start();
        }
    }
}
