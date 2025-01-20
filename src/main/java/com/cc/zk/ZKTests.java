package com.cc.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKTests {

    final static Logger log = LoggerFactory.getLogger(ZKTests.class);

    public static final String ZOOKEEPER_SERVER = "127.0.1.1:2181";

    private CuratorFramework curatorFramework = null;

    public void init() {
        if (curatorFramework != null) {
            return;
        }

        //创建zookeeper客户端
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZOOKEEPER_SERVER)
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5)) //创建重试策略
                .namespace("admin")
                .build();

        curatorFramework.start();

        try {
            if (curatorFramework.checkExists().forPath("/bgm") == null) {
                curatorFramework.create().creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath("/bgm");
                log.info("zookeeper初始化成功");
            }
        } catch (Exception e) {
            log.error("zookeeper初始化失败");
            e.printStackTrace();
        }
    }

    /**
     * 创建节点
     *
     * @throws Exception
     */
    @Test
    void createNode() throws Exception {
        // 添加持久节点
        String path = curatorFramework.create().forPath("/curator-node");
        System.out.println(String.format("curator create node :%s successfully.", path));

        // 添加临时序号节点,并赋值数据
        String path1 = curatorFramework.create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath("/curator-node", "some-data".getBytes());
        System.out.println(String.format("curator create node :%s successfully.", path));

        // System.in.read()目的是阻塞客户端关闭，我们可以在这期间查看zk的临时序号节点
        // 当程序结束时候也就是客户端关闭的时候，临时序号节点会消失
        System.in.read();
    }

    /**
     * 获取节点
     *
     * @throws Exception
     */
    @Test
    public void testGetData() throws Exception {
        // 在上面的方法执行后，创建了curator-node节点，但是我们并没有显示的去赋值
        // 通过这个方法去获取节点的值会发现，当我们通过Java客户端创建节点不赋值的话默认就是存储的创建节点的ip
        byte[] bytes = curatorFramework.getData().forPath("/curator-node");
        System.out.println(new String(bytes));
    }

    /**
     * 修改节点数据
     *
     * @throws Exception
     */
    @Test
    public void testSetData() throws Exception {
        curatorFramework.setData().forPath("/curator-node", "changed!".getBytes());
        byte[] bytes = curatorFramework.getData().forPath("/curator-node");
        System.out.println(new String(bytes));
    }

    /**
     * 创建节点同时创建⽗节点
     *
     * @throws Exception
     */
    @Test
    public void testCreateWithParent() throws Exception {
        String pathWithParent = "/node-parent/sub-node-1";
        String path = curatorFramework.create().creatingParentsIfNeeded().forPath(pathWithParent);
        System.out.println(String.format("curator create node :%s successfully.", path));
    }

    /**
     * 删除节点(包含子节点)
     *
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        String pathWithParent = "/node-parent";
        curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(pathWithParent);
    }
}
