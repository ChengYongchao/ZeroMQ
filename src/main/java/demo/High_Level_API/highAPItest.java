package demo.High_Level_API;

import demo.ZHelper;
import org.zeromq.*;

public class highAPItest {
    private static final int NBR_CLIENTS = 10;
    private static final int NBR_WORKERS = 3;
    private static byte[] WORKER_READY = {'\001'};

    private static class workertask implements Runnable {
        ZContext context = null;

        workertask(ZContext cxt) {
            this.context = cxt;
        }

        @Override
        public void run() {
            try {
                System.out.println("worker start:");
                ZMQ.Socket worker = context.createSocket(SocketType.DEALER);
                //ZHelper.setId(worker); //  Set a printable identity

                worker.connect("tcp://*:5556");


          /*      while (true) {
                    worker.send("hello");
                    String msg = worker.recvStr();
                    System.out.println("work received:" + msg);
                }*/


                //  Tell backend we're ready for work
              /*  ZFrame frame = new ZFrame(WORKER_READY);
                frame.send(worker, 0);

                while (true) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if (msg == null)
                        break;

                    msg.getLast().reset("OK");
                    msg.send(worker);
                }*/

                while (true) {
                    ZFrame frame = new ZFrame("");
                    ZMsg sendmsg = new ZMsg();

                    sendmsg.add(frame);

                    // sendmsg.add("sss");
                    sendmsg.add("I'm  worker");
                    sendmsg.send(worker);


                    //frame.send(worker, 0);
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if (msg == null) {
                        break;
                    }
                    System.out.println("msg.getFirst():" + msg.getFirst());
                    System.out.println("msg.getlast():" + msg.getLast() + " size: " + msg.size());

                }

            } catch (Exception e) {

            }
        }
    }

    private static class clienttask implements Runnable {
        ZContext context = null;

        clienttask(ZContext cxt) {
            this.context = cxt;
        }


        @Override
        public void run() {

            try {
                System.out.println("client start:");
                ZMQ.Socket client = context.createSocket(SocketType.ROUTER);
                // ZHelper.setId(client); //  Set a printable identity

                client.bind("tcp://*:5556");


                //  Send request, get reply

                ZMQ.Poller poller = context.createPoller(1);
                poller.register(client, ZMQ.Poller.POLLIN);


                while (true) {
                  /*  String reply = client.recvStr();
                    System.out.println("Client received: " + reply);
                    client.send("HELLO");*/

                    // 设置轮询时间
                    int rc = poller.poll(1000);
                    if (rc == -1)
                        break; //  Interrupted

                    if (poller.pollin(0)) {
                        System.out.println(" poller test");

                        ZMsg msg = ZMsg.recvMsg(client);
                        if (msg == null) {
                            break;
                        }
                        if (msg.size() == 3) {
                            // ZFrame msg1 = msg.unwrap();
                            int size2 = msg.size();
                            //msg.wrap(new ZFrame("hello2"));
                            msg.getLast().reset("I changed");
                            msg.send(client);
                        }
                        //System.out.println("msg.getFirst():" + msg.getFirst());


                    }
                }


            } catch (Exception e) {

            }


        }
    }


    public static void main(String[] args) {
        ZContext ctx = new ZContext();
        Thread thread1 = new Thread(new clienttask(ctx));
        Thread thread2 = new Thread(new workertask(ctx));

        thread1.start();
        thread2.start();

    }
}
