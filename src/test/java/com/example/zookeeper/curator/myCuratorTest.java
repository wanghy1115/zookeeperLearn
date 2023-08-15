package com.example.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.Bean;

import javax.swing.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class myCuratorTest {

    private CuratorFramework client;


    /**
     * 建立连接
     */
    @BeforeEach
    public void testConnection(){
        // connectString – list of servers to connect to sessionTimeoutMs – session timeout connectionTimeoutMs – connection timeout retryPolicy – retry policy to use
        //第一种方式
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
        client.start();
    }

    @AfterEach
    public void close(){
        if(client != null){
            client.close();
        }
    }

    /**
     * 创建节点
     * create -e  -s
     * 1、基本创建    create().forPath
     * 2、带数据的节点    .forPath(name, value)
     * 3、设置节点类型    .withMode()
     * 4、创建多级节点    .creatingParentsIfNeeded()
     */
    @Test
    public void testCreate() throws Exception {
        //如果创建节点没有制定数据，默认存储当前机器的IP
        client.create().forPath("/app2");
    }
    @Test
    public void testCreate1() throws Exception {
        //如果创建节点没有制定数据，默认存储当前机器的IP
        client.create().forPath("/app3","hehe".getBytes());
    }
    @Test
    public void testCreate2() throws Exception {
        //默认类型为持久化节点
        //临时节点，无法在终端看到，因为回话结束后删除了
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/app4","hehe".getBytes());
        Thread.sleep(10000);
    }
    @Test
    public void testCreate3() throws Exception {
        //创建多级节点,不允许在父节点不存在时创建子节点
        //client.create().forPath("/app5/p1");
        client.create().creatingParentsIfNeeded().forPath("/app5/p1");
    }

    /**
     * 查询节点
     * 1、查询数据：get    .getData().forPath
     * 2、查询子节点：ls    .getChildren()
     * 3、查询详细信息：ls -s
     */
    @Test
    public void testSelect() throws Exception {
        byte[] app1s = client.getData().forPath("/app1");
        System.out.println(new String(app1s));
    }
    @Test
    public void testSelect2() throws Exception {
        List<String> app5 = client.getChildren().forPath("/");
        app5.forEach(System.out::println);
    }
    @Test
    public void testSelect3() throws Exception {
        Stat status = new Stat();
        client.getData().storingStatIn(status).forPath("/app1");
        System.out.println(status.toString());
    }
    /**
     * 修改节点
     */
    @Test
    public void testUpdate() throws Exception {
        client.setData().forPath("/app1", "keyi".getBytes());
    }
    @Test
    //类似于乐观锁
    public void testUpdateForVersion() throws Exception {
        int version;
        Stat status = new Stat();
        //获取version
        client.getData().storingStatIn(status).forPath("/app1");
        version = status.getVersion();
        //写入时会将version+1写入
        client.setData().withVersion(version).forPath("/app1", "zangzang".getBytes());
    }
    /**
     * 删除节点
     * 1、普通的delete删除    .delete().forPath
     * 2、有子节点的删除deleteAll    .deletingChildrenIfNeeded()
     * 3、必须成功的删除    .guaranteed()
     * 4、回调    .inBackground() 不一定可以成功，debug可以成功，run不行
     */
    @Test
    public void testDelete() throws Exception {
        client.delete().forPath("/app1");
    }
    @Test
    public void testDelete1() throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath("/app5");
    }
    @Test
    public void testDelete2() throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath("/app5");
    }
    @Test
    public void testDelete3() throws Exception {
        //必须删除，其实就是删除失败之后重试
        client.delete().guaranteed().forPath("/app2");
    }
    @Test
    public void testDelete4() throws Exception {
        //写一个回调函数，删完后会自动调用回调函数
        client.delete().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                int i =1;
                System.out.println("节点已经删除成功"+i);
            }
        }).forPath("/app1");
    }

    /**
     * 给指定的一个节点注册监听器   NodeCache
     */
    @Test
    public void testWatcher() throws Exception {
        //1、NodeCache对象
        NodeCache nodeCache= new NodeCache(client, "/app1");
        //2、注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了");
                //获取修改节点后的数据
                byte[] data = nodeCache.getCurrentData().getData();
                System.out.println(new String(data));
            }
        });
        //3、开启监听
        //如果设置为true则开启监听时加载缓存数据
        nodeCache.start(true);

        while (true){}
    }
    /**
     *PathChildrenCache,监听某个节点的所有子节点
     */
    @Test
    public void testWatcher1() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(client, "/app2", true);
        //绑定监听器
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点也变化了");
                //监听子节点数据变更，变更之后拿到新的数据
                //1、获取变更类型
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                //判断变更是否为update
                if(type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                    //PathChildrenCacheEvent{type=CHILD_UPDATED, data=ChildData{path='/app2/d1', ......data=[49, 50]}}
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    System.out.println(new String(data));
                }
            }
        });
        cache.start();
        while (true){}
    }
    /**
     * TreeCache,监听某个节点和所有的子节点
     */
    @Test
    public void testWatcher2() throws Exception {
        //1、NodeCache对象
        TreeCache nodeCache= new TreeCache(client, "/app2");
        //2、注册监听
        nodeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("发生了变化");
            }
        });
        //3、开启监听
        //如果设置为true则开启监听时加载缓存数据
        nodeCache.start();
        while (true){}
    }
}
