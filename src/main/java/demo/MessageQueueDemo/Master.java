package demo.MessageQueueDemo;

import org.omg.PortableInterceptor.INACTIVE;
import org.zeromq.*;
import zmq.ZError;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class Master implements Runnable {

    private final static int HEARTBEAT_INTERVAL = 1000;
    private final static String WORKER_READY = "\001"; //  Signals worker is ready
    private final static String WORKER_RESULT = "\002"; //  worker计算完成返回消息标志
    private final static String MASTER_TASK = "\003";   //主节点发送任务消息标志
    private final static String MASTER_VERTEX = "\004";   //主节点发送顶点消息标志

    private ZContext context = new ZContext();
    private ZMQ.Socket Master = null;
    private ZMQ.Poller poller = null;
    private WorkerQueue workerQueue = new WorkerQueue();

    private final List<List<Integer>> workerVertices = new ArrayList<>();   //顶点集
    private int index = 0;
    private Map<Integer, ZFrame> IdToAddress = new HashMap<>(); //节点和顶点集的对应关系
    private Map<Integer, Integer> result = new HashMap<>();  //结果集
    private Boolean isVertexSend = false;

    public Master() {

        try {
            Master = context.createSocket(SocketType.ROUTER);
            Master.bind("tcp://*:5556"); //  For workers
            poller = context.createPoller(1);
            poller.register(Master, ZMQ.Poller.POLLIN);

        } catch (Exception e) {
            System.err.println("绑定端口失败");
        }


    }


    // 监听方法.
    @Override
    public void run() {
        System.out.println("proxy is start:");
        try {

            while (true) {

                int rc = poller.poll(HEARTBEAT_INTERVAL);
                if (rc == -1)
                    break; //  Interrupted

                // 获取 worker响应
                if (poller.pollin(0)) {

                    ZMsg msg = ZMsg.recvMsg(Master);
                    if (msg == null)
                        break; //  Interrupted

                    //  获取msg address，根据address区分不同worker（也可设置uid）
                    ZFrame address = msg.unwrap();


                    //获取标志位signal
                    ZFrame frame = msg.getFirst();
                    String signal = new String(frame.getData(), ZMQ.CHARSET);

                    // 判断是否是ready消息
                    if (WORKER_READY.equals(signal)) {
                        workerQueue.addWorker(address);
                        System.out.println("worker:" + address + "is ready");
                    } else if (WORKER_RESULT.equals(signal)) { //判断是否是结果消息
                        Map<Integer, Integer> taskResult = (HashMap) serializeToObject(msg.getLast().getData());
                        result.putAll(taskResult);
                        int sum = 0;
                        for (Integer keyValue : result.values()) {
                            sum += keyValue;
                        }

                        if (sum > 0) {
                            System.out.println("=======>结果:" + sum + "大于0，继续计算");
                            sendTask();

                        } else {
                            System.out.println("=======>结果:" + sum + "小于0，停止计算");
                            return;
                        }

                    }
                    msg.destroy();
                    continue;
                }
                //  开始发顶点和任务 只发一次
                if (!isVertexSend && workerQueue.size() > 0) {

                    //初始化顶点集
                    //  初始值
                    int id = 0;
                    for (Integer num = workerQueue.size(); num > 0; num--) {
                        List<Integer> vertices = new ArrayList<>();

                        for (int i = 5; i > 0; i--) {
                            vertices.add(id);
                            result.put(id++,1000);
                        }
                        workerVertices.add(vertices);
                    }
                    this.index = workerVertices.size() - 1;


                    //从节点可能已经连接，判断节点数 开始发送顶点
                    sendVertex();
                    //顶点发送完，若没有消息则认为节点已接收到顶点，开始发送任务
                    sendTask();
                }
            }
        } catch (IOException e) {

            System.err.println("序列化失败");

        } catch (Exception e) {

        } finally {

        }
    }


    /*
     * todo 发送任务方法
     *
     * */
    public void sendTask() throws IOException {
        for (ZFrame value : IdToAddress.values()) {
            ZFrame address = new ZFrame(value.getData());
            ZMsg taskMsg = new ZMsg();
            taskMsg.add(address);
            taskMsg.add(MASTER_TASK);
            taskMsg.add(serialize(result));
            taskMsg.send(Master);
            System.out.println("发送结果集======>");
        }


    }

    public void sendVertex() throws IOException, ClassNotFoundException {
        if (!isVertexSend) {
            //todo 只发送一次。
            // 有可用worker 则发送顶点集
            System.out.println("wokers num:" + workerQueue.size());

            while (workerQueue.size() > 0 && index >= 0) {
                ZMsg sendTaskMsg = new ZMsg();
                ZFrame address = new ZFrame(workerQueue.next().getData());
                // 设置address 和顶点集映射关系
                IdToAddress.put(index, new ZFrame(address.getData()));

                sendTaskMsg.add(address);
                sendTaskMsg.add(MASTER_VERTEX);
                sendTaskMsg.add(serialize(workerVertices.get(index)));

                System.out.println("=======>发送任务" + (String) serializeToObject(serialize(workerVertices.get(index))).toString() + "address:" + sendTaskMsg.getFirst());
                sendTaskMsg.send(Master);


                index--;
            }
            //只发送一次
            isVertexSend = true;
        }
    }

    /**
     * 序列化
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();

        // String string = byteArrayOutputStream.toString("GBK");
        objectOutputStream.close();
        byteArrayOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }


    /**
     * 反序列化
     */
    public static Object serializeToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        byteArrayInputStream.close();
        return object;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Thread master = new Thread(new Master());
        master.start();
     /* List<Integer> test = new ArrayList<>();
      for(int i =10; i >0; i--){
          test.add(i);
      }
      byte [] incode = serialize(test);
      List res = (ArrayList)serializeToObject(incode);
      System.out.println(res.toString());*/
    }
}
