package com.cc.zk;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.zookeeper.server.quorum.QuorumPeer.ServerState.*;

public class ZkServer {

    private static final Logger log = LoggerFactory.getLogger(ZkServer.class);

    static Map<Integer, String> allElections = new HashMap<>();

    boolean running = true;
    QuorumPeer.ServerState peerState = LOOKING;

    int myId;
    String electionHost;
    int electionPort;

    int currentEpoch = 1;

    LinkedBlockingQueue<ToSend> sendqueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Notification> recvqueue = new LinkedBlockingQueue<>();

    Map<Integer, Integer> sidAndVote = new HashMap<>();
    private int leaderId = -1;

    public static void main(String[] args) throws Exception {
        allElections.put(1, "127.0.0.1:3181");
        allElections.put(2, "127.0.0.1:3182");
        allElections.put(3, "127.0.0.1:3183");

        new ZkServer(Integer.parseInt(args[0])).start();
    }

    public ZkServer(int myId) {
        this.myId = myId;
    }

    private void start() throws Exception {
        String[] hostAndPort = allElections.get(myId).split(":");
        electionHost = hostAndPort[0];
        electionPort = Integer.parseInt(hostAndPort[1]);

        log.debug("Starting quorum peer");

        init();

        while (running) {
            switch (peerState) {
                case LOOKING:
                    log.info("LOOKING");
                    try {
                        doLooking();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case OBSERVING:
                    log.info("OBSERVING");
                    break;
                case FOLLOWING:
                    log.info("FOLLOWING");
                    running = true;
                    break;
                case LEADING:
                    log.info("LEADING");
                    running = true;
                    break;
            }
            Thread.sleep(200);
            break;
        }
    }

    /**
     * 初始化收发线程
     */
    private void init() {
        new Thread(() -> {
            try {
                send();
            } catch (Exception e) {
                running = false;
                e.printStackTrace();
            }
        }, "thread-send").start();

        new Thread(() -> {
            try {
                receive();
            } catch (Exception e) {
                running = false;
                e.printStackTrace();
            }
        }, "thread-receive").start();

        new Thread(() -> {
            try {
                receiveSocket();
            } catch (Exception e) {
                running = false;
                e.printStackTrace();
            }
        }, "thread-receiveSocket").start();
    }

    private void doLooking() throws Exception {
        // 投自己一票
        sendqueue.offer(new ToSend().setMyId(myId).setProposedLeaderId(myId).setCurrentEpoch(currentEpoch).setSid(myId));

        for (Integer sid : allElections.keySet()) {
            if (sid != myId) {
                ToSend toSend = new ToSend().setMyId(myId).setProposedLeaderId(myId).setCurrentEpoch(currentEpoch).setSid(sid);
                sendqueue.offer(toSend);
                log.debug("sendqueue.offer toSend = {}", toSend);
            }
        }
    }

    private void send() throws Exception {
        log.debug("send, start");
        Map<Integer, Socket> sockets = new HashMap<>();

        while (running) {
            switch (peerState) {
                case LOOKING:
                case OBSERVING:
                case FOLLOWING:
                case LEADING:
                    ToSend toSend = sendqueue.poll(200, TimeUnit.MILLISECONDS);
                    if (toSend == null) {
                        continue;
                    }
                    log.debug("send, sendqueue.poll = {}", toSend);

                    int toSendMyId = toSend.getMyId();
                    int toSendProposedLeaderId = toSend.getProposedLeaderId();
                    int toSendCurrentEpoch = toSend.getCurrentEpoch();
                    int toSendSid = toSend.getSid();

                    // 发送给自己
                    if (toSendSid == myId) {
                        log.debug("send, to myself, recvqueue.offer");
                        recvqueue.offer(new Notification()
                                .setState(peerState)
                                .setMyId(toSendMyId)
                                .setProposedLeaderId(toSendProposedLeaderId)
                                .setCurrentEpoch(toSendCurrentEpoch)
                                .setSid(toSendSid));
                        continue;
                    }

                    // 创建连接
                    Socket socket = sockets.get(toSendSid);
                    if (socket == null) {
                        String[] hostAndPort = allElections.get(toSendSid).split(":");
                        try {
                            socket = new Socket(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                        } catch (Exception e) {
                            log.error("发送投票，创建连接异常, host = {}, port = {}", hostAndPort[0], Integer.parseInt(hostAndPort[1]), e);
                            continue;
                        }
                        sockets.put(toSendMyId, socket);
                    }

                    // 发送数据
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    ByteBuffer byteBuffer = buildMsg(peerState, toSendMyId, toSendProposedLeaderId, toSendCurrentEpoch, toSendSid);
                    out.writeInt(byteBuffer.capacity());
                    out.write(byteBuffer.array());
                    out.flush();
                    log.info("send success: toSendMyId = {}, toSendProposedLeaderId = {}, toSendCurrentEpoch = {}, toSendSid = {}",
                            toSendMyId, toSendProposedLeaderId, toSendCurrentEpoch, toSendSid);
            }
        }
    }

    private void receiveSocket() throws Exception {
        log.debug("receiveSocket, start");
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(electionPort));

        while (running) {
            Socket client = ss.accept();
            client.setTcpNoDelay(true);
            client.setKeepAlive(/*tcpKeepAlive*/true);
            client.setSoTimeout(/*socketTimeout*/200 * 3);

            DataInputStream din = new DataInputStream(
                    new BufferedInputStream(client.getInputStream()));
            int length = din.readInt();
            if (length <= 0 || length > 524288) {
                throw new IOException(
                        "Received packet with invalid packet: "
                                + length);
            }
            /**
             * Allocates a new ByteBuffer to receive the message
             */
            byte[] msgArray = new byte[length];
            din.readFully(msgArray, 0, length);
            ByteBuffer message = ByteBuffer.wrap(msgArray);
            // addToRecvQueue(new QuorumCnxManager.Message(message.duplicate(), sid));

            // 读取
            message.clear();
            int stateInt = message.getInt();
            int myId = message.getInt();
            int proposedLeaderId = message.getInt();
            int currentEpoch = message.getInt();
            int sid = message.getInt();

            // State of peer that sent this message
            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
            switch (stateInt) {
                case 0:
                    ackstate = QuorumPeer.ServerState.LOOKING;
                    break;
                case 1:
                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                    break;
                case 2:
                    ackstate = QuorumPeer.ServerState.LEADING;
                    break;
                case 3:
                    ackstate = QuorumPeer.ServerState.OBSERVING;
                    break;
            }
            Notification notification = new Notification().setState(ackstate).setMyId(myId).setProposedLeaderId(proposedLeaderId).setCurrentEpoch(currentEpoch).setSid(sid);

            switch (peerState) {
                case LOOKING:
                    log.info("reveive success, LOOKING, notification = {}", notification);
                    recvqueue.offer(notification);
                    break;
                case OBSERVING:
                    log.info("reveive success, OBSERVING, notification = {}", notification);
                    break;
                case FOLLOWING:
                    log.info("reveive success, FOLLOWING, notification = {}", notification);
                    if (notification.getProposedLeaderId() != this.leaderId) {
                        sendqueue.offer(new ToSend().setMyId(this.myId).setProposedLeaderId(this.leaderId).setCurrentEpoch(currentEpoch).setSid(myId));
                    }
                    break;
                case LEADING:
                    log.info("reveive success, LEADING, notification = {}", notification);
                    sendqueue.offer(new ToSend().setMyId(this.myId).setProposedLeaderId(this.myId).setCurrentEpoch(currentEpoch).setSid(myId));
                    break;
            }
        }
    }

    private void receive() throws Exception {
        log.debug("receive, start");

        while (running) {
            switch (peerState) {
                case LOOKING:
                    Notification notification = recvqueue.poll(200, TimeUnit.MILLISECONDS);
                    if (notification == null) {
                        continue;
                    }
                    log.debug("send, sendqueue.poll = {}", notification);

                    QuorumPeer.ServerState state = notification.getState();
                    int myId = notification.getMyId();
                    int proposedLeaderId = notification.getProposedLeaderId();
                    int currentEpoch = notification.getCurrentEpoch();
                    int sid = notification.getSid();

                    if (currentEpoch < this.currentEpoch) {
                        log.debug("receive continue, their currentEpoch = {} < this.currentEpoch = {}, sidAndVote = {}", currentEpoch, this.currentEpoch, sidAndVote);
                        continue;
                    }
                    if (currentEpoch > this.currentEpoch) {
                        this.currentEpoch = currentEpoch;
                        sidAndVote.clear();
                        sidAndVote.put(myId, proposedLeaderId);
                        log.debug("receive continue, their currentEpoch = {} > this.currentEpoch = {}, sidAndVote = {}", currentEpoch, this.currentEpoch, sidAndVote);
                        continue;
                    }

                    if (state == LEADING || state == FOLLOWING) {
                        log.debug("receive update sidAndVote, their state = {} > this.state = {}, sidAndVote = {}", state, this.peerState, sidAndVote);
                        sidAndVote.put(this.myId, proposedLeaderId);
                        sidAndVote.put(myId, proposedLeaderId);
                    } else {
                        // 比较 myId
                        if (myId < this.myId) {
                            if (proposedLeaderId != sidAndVote.get(this.myId)) {
                                log.debug("receive continue, their myId = {} < this.myId = {}, sidAndVote = {}", myId, this.myId, sidAndVote);
                                sendqueue.offer(new ToSend().setMyId(this.myId).setProposedLeaderId(sidAndVote.get(this.myId)).setCurrentEpoch(this.currentEpoch).setSid(myId));
                                continue;
                            }
                            log.debug("receive update sidAndVote, their myId = {} < this.myId = {}, sidAndVote = {}", myId, this.myId, sidAndVote);
                            sidAndVote.put(myId, proposedLeaderId);
                        } else if (myId == this.myId) {
                            log.debug("receive update sidAndVote, their myId = {} = this.myId = {}, sidAndVote = {}", myId, this.myId, sidAndVote);
                            sidAndVote.put(this.myId, proposedLeaderId);
                        } else {
                            log.debug("receive update sidAndVote, their myId = {} > this.myId = {}, sidAndVote = {}", myId, this.myId, sidAndVote);
                            sidAndVote.put(this.myId, proposedLeaderId);
                            sidAndVote.put(myId, proposedLeaderId);
                        }
                    }

                    log.debug("receive before decide, sidAndVote = {}", sidAndVote);

                    // 判断是否结束选举
                    int minWinNum = allElections.size() / 2 + 1;
                    AtomicInteger winLeaderId = new AtomicInteger(-1);
                    HashMap<Integer, Integer> proposedLeaderIdAndNum = new HashMap<>();
                    for (Map.Entry<Integer, Integer> entry : sidAndVote.entrySet()) {
                        int sId = entry.getKey();
                        int leaderId = entry.getValue();

                        /// 获得的票数
                        Integer num = proposedLeaderIdAndNum.get(leaderId);
                        if (num == null) {
                            num = 0;
                        }
                        num++;
                        if (num >= minWinNum) {
                            winLeaderId.set(leaderId);
                            break;
                        }
                        proposedLeaderIdAndNum.put(leaderId, num);
                    }
                    leaderId = winLeaderId.get();
                    log.debug("receive after decide, leaderId = {}", leaderId);
                    if (leaderId > 0) {
                        peerState = leaderId == this.myId ? QuorumPeer.ServerState.LEADING : QuorumPeer.ServerState.FOLLOWING;
                        for (Integer sidEle : allElections.keySet()) {
                            if (sidEle != this.myId) {
                                sendqueue.offer(new ToSend().setMyId(this.myId).setProposedLeaderId(leaderId).setCurrentEpoch(currentEpoch).setSid(sidEle));
                            }
                        }
                        sidAndVote.clear();
                        log.info("---------- ELECTION END, I'm {}, leaderid = {}", peerState, leaderId);
                        break;
                    }
                case OBSERVING:
                case FOLLOWING:
                case LEADING:
                    break;
            }
        }
    }

    static ByteBuffer buildMsg(QuorumPeer.ServerState state, int toSendMyId, int toSendProposedLeaderId, int toSendCurrentEpoch, int toSendSid) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state.ordinal());
        requestBuffer.putInt(toSendMyId);
        requestBuffer.putInt(toSendProposedLeaderId);
        requestBuffer.putInt(toSendCurrentEpoch);
        requestBuffer.putInt(toSendSid);
        return requestBuffer;
    }

    @Data
    @Accessors(chain = true)
    private static class ToSend {
        private int myId;
        private int proposedLeaderId;
        private int currentEpoch;
        private int sid;
    }

    @Data
    @Accessors(chain = true)
    private static class Notification {
        private QuorumPeer.ServerState state;
        private int myId;
        private int proposedLeaderId;
        private int currentEpoch;
        private int sid;
    }
}
