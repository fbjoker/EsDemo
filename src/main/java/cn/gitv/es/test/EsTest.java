package cn.gitv.es.test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.name.Names;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import com.alibaba.fastjson.JSON;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EsTest {
    public  static TransportClient client = null;
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private static ExecutorService queueService = Executors.newCachedThreadPool();
    private static AtomicInteger syncount=new AtomicInteger(0);
    private static AtomicLong writecount=new AtomicLong(0);
    private static AtomicLong syntime=new AtomicLong(0);
    private static ConcurrentHashMap<String,Long> times=new ConcurrentHashMap<String,Long>();
    private static final Queue <List<Map<String,Object>>> queue = new ConcurrentLinkedQueue <List<Map<String,Object>>>();
    public static void main(String args[]){
        try {
            //单条写入测试
            //indexTest(1,"s4");
            //批量写入测试
            deleteIndex("bulktest4");
            createIndex("bulktest4");
            createMapping("bulktest4");
           // createDataMapping("bulktest5");
           // createDataMapping20("bulktest5");
            System.setProperty("log4j.configuration", "log4j.properties");
            if(args.length<6){
                System.out.println("请输入参数：index,rows,num,thread,wthread,total");
                System.exit(0);
            }
            String index= args[0];
            int rows = Integer.parseInt(args[1]);
            int num  = Integer.parseInt(args[2]);
            int thread  = Integer.parseInt(args[3]);
            int wthread  = Integer.parseInt(args[4]);
            Long total  = Long.parseLong(args[5]);
           // bulkmap(index,rows,num);
            //           // bulkThread(index,rows,num,thread);
            try {
                System.out.println("start create data ---------------------");
                Producer p = new Producer(queue);
                p.setIndex(index);
                p.setRows(rows);
                p.setNum(num);
                p.setThread(wthread);
                new Thread(p).start();
              //  System.out.println("start write data ---------------------");
                Thread t = new Thread(new Runnable(){
                    public void run(){
                        //System.out.println("start write data ---------------------");
                        Long starttime=System.currentTimeMillis();
                        writeThread(index,thread,total);
                        Long endtime=System.currentTimeMillis();
                        System.out.println("write data="+(endtime-starttime)/1000);
                    }
                });
                t.start();

            }catch (Exception e){
                e.printStackTrace();
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void indexTest(int num,String time){
        Long starttime=System.currentTimeMillis();
        for(int n=0;n<num;n++){
            indexCreate(time+(num+n));
        }
        Long endtime=System.currentTimeMillis();
        System.out.println("indextime="+(endtime-starttime)/1000);
    }

    public static void bulkTest(int num,String time){
        String [] ids=new String[num];
        for(int n=0;n<num;n++){
            ids[n]=time+(num+n);
        }
        Long starttime=System.currentTimeMillis();
        bulks(ids);
        Long endtime=System.currentTimeMillis();
        System.out.println("bulktime="+(endtime-starttime)/1000);
    }

    public static void bulkNum(int num,String time){
        String [] ids=new String[num];
        for(int n=0;n<num;n++){
            ids[n]=time+(num+n);
        }
        Long starttime=System.currentTimeMillis();
        bulk(ids);
        Long endtime=System.currentTimeMillis();
        System.out.println("bulktime="+(endtime-starttime)/1000);
    }

    public static  void bulkdata(){
        //批量写入测试
        int n=20;
        while (true){
            try {
                n=n+1;
                bulkTest(50000,"sss"+n);
                SearchResponse searchResponse=queryAll();
                long count=searchResponse.getHits().getTotalHits();
                System.out.println("count:"+ count);
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    public static synchronized TransportClient getClient() throws UnknownHostException{
        //TransportClient client = null;
        if(client==null){
             {
                Settings settings = Settings.builder()
                        .put("client.transport.sniff", true)
                        .put("client.transport.ping_timeout","60s")
                        //.put("cluster.name", "es-test")
                        .put("cluster.name", "essearch-test")
                        .build();

                //  .put(Names.named(), new FixedExecutorBuilder(settings, "bulk", availableProcessors, 200))
               /* client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.121.119"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.121.120"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.121.121"), 9300));*/
                 client = new PreBuiltTransportClient(settings)
                         .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.130.151"), 9300))
                      /*   .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.130.152"), 9300))
                         .addTransportAddress(new TransportAddress(InetAddress.getByName("10.10.130.153"), 9300))*/;

            }
         }
        return client;
    }

    public static IndexResponse indexCreate(String id){
        IndexResponse response=null;
        try {
            TransportClient client= getClient();
            response = client.prepareIndex("bulktest1", "_doc", id)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("albumid", id)
                            .field("postDate", new Date().getTime())
                            .field("albumname", "刘家媳妇1")
                            .field("parntner", "SD_CMCC_JN")
                            .field("action", 5)
                            .field("play", 100)
                            .endObject()
                    )
                    .get();
        }catch (Exception  e){
            e.printStackTrace();
        }
        return response;
    }


    public static  void bulkmap(String index,int rows,int num) {
        try {
            while (true){
                Long starttime=System.currentTimeMillis();
                TransportClient client= getClient();
                BulkRequestBuilder bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                List<Map<String,Object>> allData =getAllData(rows,num,index);
                for(Map<String,Object> data:allData){
                    bulkRequest.add(client.prepareIndex(index, "_doc", data.get("partner").toString()+data.get("mac").toString())
                            .setSource(data)
                    );
                }
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    // process failures by iterating through each bulk response item
                    System.out.println("error-------");
                }
                Long endtime=System.currentTimeMillis();
                System.out.println("excute time:"+(endtime-starttime)/1000);
                Thread.sleep(1000);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static  void bulkThread(String index,int rows,int num,int thread) {

            while (true){
                times.put("maxtimes",0L);
                times.put("mintimes",0L);
                for(int n=0;n<thread;n++){
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Long starttime=System.currentTimeMillis();
                                TransportClient client= getClient();
                                BulkRequestBuilder bulkRequest = client.prepareBulk();
                                List<Map<String,Object>> allData =getAllData(rows,num,index);
                                for(Map<String,Object> data:allData){
                                    bulkRequest.add(client.prepareIndex(index, "_doc")
                                            .setSource(data)
                                    );
                                }
                                BulkResponse bulkResponse = bulkRequest.get();
                                if (bulkResponse.hasFailures()) {
                                    System.out.println("error:"+bulkResponse.buildFailureMessage());
                                }

                                Long endtime=System.currentTimeMillis();
                                //System.out.println(Thread.currentThread().getName()+" excute time:"+bulkResponse.getTook().getMillis());
                                //System.out.println(Thread.currentThread().getName()+" excute time:"+(endtime-starttime)/1000);
                                syntime.getAndAdd(bulkResponse.getTook().getMillis());
                                if(bulkResponse.getTook().getMillis()>times.get("maxtimes")){
                                    times.put("maxtimes",bulkResponse.getTook().getMillis());
                                }

                                if(times.get("mintimes")==0){
                                    times.put("mintimes",bulkResponse.getTook().getMillis());
                                }else if(bulkResponse.getTook().getMillis()<times.get("mintimes")){
                                    times.put("mintimes",bulkResponse.getTook().getMillis());
                                }
                                syncount.getAndIncrement();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    });
                }
                while (true){
                    if(syncount.get()<thread){
                        try {
                            Thread.sleep(1000);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }else{
                        syncount.set(0);
                        System.out.println(new Date()+":min time:"+times.get("mintimes")+" ,max time:"+times.get("maxtimes")+" ，took time:"+syntime.get()/thread);
                        syntime.set(0);
                        break;
                    }
                }
            }


    }


    // 创建索引
    public static void createIndex(String INDEX){
        try {
            TransportClient client= getClient();
           // client.admin().indices().prepareCreate(INDEX).get();
            client.admin().indices().prepareCreate(INDEX)
                    .setSettings(Settings.builder()
                    .put("index.number_of_shards",3)
                    .put("index.number_of_replicas", 0)
                  //  .put("index.translog.durability","async")
                   // .put("index.translog.durability","request")
                  //  .put("index.merge.policy.max_merged_segment","2gb")
                 //   .put("index.merge.policy.segments_per_tier","24")
                   // .put("index.refresh_interval","300s")
                 //   .put("index.translog.flush_threshold_size","1mb")
                    .put("index.translog.sync_interval","100ms")
                    ).get();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    // 删除索引
    public static boolean deleteIndex(String INDEX){
        boolean isdel=true;
        try {
            TransportClient client= getClient();
            AcknowledgedResponse res=client.admin().indices().prepareDelete(INDEX).get();
            isdel= res.isAcknowledged();
           }catch (Exception e){
            e.printStackTrace();
        }
        return isdel;
    }
    //生成多条数据
    public static List<Map<String,Object>> getAllData(int rows, int num,String index){
        List<Map<String,Object>> allData=new ArrayList<Map<String,Object>>();
        for(int n=0;n<rows;n++){
            if("bulktest4".equalsIgnoreCase(index)){
                allData.add(createDataNew());
            }else{
                allData.add(createData(num));
            }

        }
        return allData;
    }

    public static  void wqueueThread(String index,int rows,int num,int thread) {
        while (true){
            for(int n=0;n<thread;n++){
                queueService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            List<Map<String,Object>> allData=new ArrayList<Map<String,Object>>();
                            for(int n=0;n<rows;n++){
                                if("bulktest4".equalsIgnoreCase(index)){
                                    allData.add(createDataNew());
                                }else{
                                    allData.add(createData(num));
                                }
                            }
                            System.out.println("allData="+allData.size());
                           // queue.put(allData);
                            queue.add(allData);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                });
            }

            try {
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static  void writeThread(String index,int thread,Long total) {
        while (true){
            for(int n=0;n<thread;n++){
                queueService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            //List<Map<String,Object>> allData =queue.take();
                            List<Map<String,Object>> allData =queue.poll();
                            //System.out.println("allData size:"+queue.size());
                            if(allData!=null){
                                TransportClient client= getClient();
                                BulkRequestBuilder bulkRequest = client.prepareBulk();
                                for(Map<String,Object> data:allData){
                                    bulkRequest.add(client.prepareIndex(index, "_doc")
                                            .setSource(data)
                                    );
                                }
                                BulkResponse bulkResponse = bulkRequest.get();
                                if (bulkResponse.hasFailures()) {
                                    System.out.println("error:"+bulkResponse.buildFailureMessage());
                                }
                                System.out.println(Thread.currentThread().getName()+" write took:"+bulkResponse.getTook().getMillis()/1000);
                                writecount.getAndAdd(allData.size());
                            }
                            syncount.getAndIncrement();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                });
            }
            while (true){
                if(syncount.get()<thread){
                    try {
                        //System.out.println("write-----------------------:"+syncount.get());
                        Thread.sleep(10);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }else{
                    System.out.println("write index-----------------------:"+writecount.get());
                    syncount.set(0);
                    break;
                }
            }
            if(writecount.get()>total){
                break;
            }
        }
    }
    //生成数据
    public static Map<String,Object>  createData(int num){
        ConcurrentHashMap<String, String> filds = new ConcurrentHashMap<String, String>();
        filds=DataMaping.getRandomValue(num);
        ConcurrentHashMap<String, Object> datamap = new ConcurrentHashMap<String, Object>();
        try {
            datamap.put("partner",getPartner());
            datamap.put("mac",createMac());
            datamap.put("albumName",getAlbumName());
            datamap.put("channelName",geChannelName());
            for(String key:filds.keySet()){
                if(filds.get(key)!=null && filds.get(key).equalsIgnoreCase("Long")){
                    datamap.put(key,getLong());
                }else if(filds.get(key)!=null && filds.get(key).equalsIgnoreCase("int")){
                    datamap.put(key,getInt());
                }else if(key.contains("Id")){
                    datamap.put(key,getLong());
                } else {
                    datamap.put(key,getValues());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return datamap;
    }

    //生成数据
    public static Map<String,Object>  createDataNew(){
       // Map<String,String> filds=DataMaping.getRandomValue(num);
        ConcurrentHashMap<String, Object> datamap = new ConcurrentHashMap<String, Object>();
        datamap.put("partner",getPartner());
        datamap.put("mac",createMac());
        datamap.put("albumName",getAlbumName());
        datamap.put("albumId",getLong());
        return datamap;
    }

    public static  String createMac(){
        Random rd=new Random();
        String SEPARATOR_OF_MAC = ":";
        String[] mac = {String.format("%02x", rd.nextInt(0xff)),
                String.format("%02x", rd.nextInt(0xff)),
                String.format("%02x",rd.nextInt(0xff)),
                String.format("%02x",rd.nextInt(0xff)),
                String.format("%02x",rd.nextInt(0xff)),
                String.format("%02x",rd.nextInt(0xff))};
        String mc=String.join(SEPARATOR_OF_MAC, mac);
        return  mc;
    }

    public static  String getPartner(){
        List<String> list = new ArrayList<String>() {{
            add("JS_CMCC_CP");
            add("AH_CMCC");
            add("ZJYD");
            add("SAXYD");
            add("SAXYD_ZX");
            add("JS_CMCC_CP_ZX");
            add("SD_CMCC_JN");
            add("SD_CMCC_QD");
            add("HN_CMCC");
            add("HNYD");
            add("HN_CMCC_YP");
            add("JSZZ");
            add("JS_CMCC_CP_ZX");
            add("YNYD");
            add("YNYD_ZX");
        }};
        Random rd=new Random();
       int num= rd.nextInt(list.size()-1);
        return list.get(num);
    }

    public static Long getLong(){
        Random rd=new Random();
        Long num= rd.nextLong();
        return num;
    }
    public static Integer getInt(){
        Random rd=new Random();
        Integer num= rd.nextInt(100000);
        return num;
    }

    public static Object getValues(){
        List<Object> list = new ArrayList<Object>() {{
            add("20190311071606289");
            add("2153016459HYG8590274");
            add("7fbf742842574fe49153e54ca8da8cc4");
            add("18865796566");
            add("EC6108V9U_pub_sdlyd");
            add("112.8.242.227");
            add("2.2.70");
            add("2019-03-11 07:16:03");
            add("vod.clientversion");
            add("vod.pingbackversion");
            add("测试数据测试数据测试");
            add("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据");

        }};
        Random rd=new Random();
        Integer num= rd.nextInt(list.size());
        return list.get(num);
    }

    public static String geChannelName(){
        List<String> list = new ArrayList<String>() {{
            add("电影");
            add("电视剧");
            add("儿童");
            add("综艺");
            add("音乐");
            add("体育");
            add("教育");
            add("片花");
            add("纪录片");
            add("娱乐");
            add("时尚");
            add("搞笑");
            add("其他");
            add("游戏");
            add("新闻");
            add("健康");
            add("军事");
            add("科技");
            add("财经");
            add("母婴");
            add("旅游");
            add("汽车");
            add("资讯");
            add("原创");
            add("生活");
            add("动漫");
        }};
        Random rd=new Random();
        Integer num= rd.nextInt(list.size());
        return list.get(num);
    }
    public static String getAlbumName(){
        List<String> list = new ArrayList<String>() {{
            add("大头儿子小头爸爸 第一季 ");
            add("铁齿铜牙纪晓岚第一部");
            add("熊出没之过年");
            add("旱码头");
            add("如果爱");
            add("记忆大师");
            add("小鬼闯巴黎");
            add("夺宝幸运星1 ");
            add("赛尔号第3季 ");
            add("小马宝莉友谊的魔力 第3季");
            add("加油吧实习生");
            add("妈妈无罪");
            add("新大头儿子小头爸爸");
            add("熊出没之丛林总动员");
            add("琅琊榜");
            add("大耳朵图图第5季 ");
            add("钢铁飞龙");
            add("大丫鬟");
            add("因为爱情有幸福之二");
            add("大头儿子小头爸爸 第二季 ");
            add("有种你爱我");
            add("贝瓦儿歌之新春特辑");
            add("阿U学科学 ");
            add("西游记的故事");
            add("来不及说我爱你");
            add("汽车总动员");
            add("变形金刚之雷霆舰队");
            add("爱情回来了");
            add("笑傲江湖第二季");
            add("猫小帅故事");
            add("爆笑动物园");
            add("芭比之梦想豪宅 第4季");
            add("宝宝巴士之奇妙好习惯");
            add("黎明之眼");
            add("葫芦兄弟");
            add("枪花");
            add("大鼻孔玩具故事之精灵宝可梦");
            add("阿狸的童话日记");
            add("迷案1937");
            add("爱探险的朵拉 第7季 英文版 ");
            add("第三种爱情");
            add("西游记之大圣归来");
            add("爱丽和故事");
            add("东北往事之破马张飞");
            add("笑动剧场");
            add("玩疯了色彩");
            add("伪装者");
            add("赛德克·巴莱 ");
            add("麻辣变形计");
            add("海尔兄弟二十周年纪念版");
            add("小羊肖恩冠军羊全集");
            add("狗十三");
            add("幼儿舞蹈系列大全");
            add("太空熊猫总动员");
            add("领袖");
            add("斗龙战士全集");
            add("中国情歌汇");
            add("无间道2 ");
            add("择天记");
            add("亲爱的活祖宗");
            add("南瓜入侵");
            add("芝麻胡同");
            add("奇奇和悦悦的玩具");
            add("小马宝莉之小马国女孩3：友谊大赛 ");
            add("超凡蜘蛛侠2 ");
            add("校花的贴身高手2 ");
            add("哈利与萌萌兔");
            add("光之美少女");
            add("超人总动员");
            add("小P优优 第3季 英文版");
            add("我是特种兵");
            add("雪地娘子军");
            add("捉妖记2 ");
            add("结婚为什么");
            add("我的男友比我美");
            add("盛夏晚晴天");
            add("小马宝莉 第7季");
            add("娜希德");
            add("赛车总动员3：极速挑战 ");
            add("《特工皇妃楚乔传》热血致敬台前幕后");
            add("哆啦A梦：伴我同行 粤语");
            add("辛普森一家");
            add("掩护");
            add("一粒红尘");
            add("战火中的兄弟");
            add("战斗王飓风战魂 第5季");
            add("明末风云第2部 ");
            add("宝宝巴士启蒙音乐剧之疯狂怪兽车");
            add("帮帮龙出动番外篇合集");
            add("玩具boy ");
            add("特种兵王2使命抉择 ");
            add("三天两夜");
            add("呜咪123 第3季 ");
            add("海底总动员2：多莉去哪儿 ");
            add("2019湖北卫视春晚");
            add("肖恩的青苔农场");
            add("猪猪侠之终极决战前夜篇");
            add("赛尔号 第7季 宇宙之眼 ");
            add("甜蜜暴击");
            add("铁道飞虎");
            add("功夫瑜伽");
            add("宝宝巴士启蒙音乐剧之恐龙世界");
            add("小伶玩具：搞笑生活");
            add("万能阿曼");
            add("怪兽电力公司 （普通话） ");
            add("巴啦啦小魔仙之飞越彩灵堡 第2季");
            add("玩疯了不吃鱼");
            add("小马宝莉友谊的魔力 第3季");
            add("前任的诱惑");
            add("鲁滨逊漂流记（4K）");
            add("西游记动画");
            add("哆啦A梦 第4季 ");
            add("空天战队");
            add("喜剧班的春天");
            add("哆啦A梦：新·大雄的日本诞生");
            add("我的世界2017");
            add("最强大脑之燃烧吧大脑第2季 ");
            add("芭比之奇幻日记");
            add("推拿");
            add("凯利迷你剧场");
            add("摇滚萝莉 第2季");
            add("士兵突击");
            add("汽车城之建筑队");
            add("小马宝莉之坎特洛特故事集");
            add("花儿与少年花絮");
            add("小马宝莉 第8季");
            add("人间规则");
            add("奥姆的故事");
            add("大话西游之大圣娶亲");
            add("普罗米修斯（3D）");
            add("小黄人与格鲁日记");
            add("爱玩具视频");
            add("飞天小女警 第6季");
            add("里约大冒险2 ");
            add("家有儿女");
            add("茶道真兄弟");
            add("兽王争锋 第2季 原石之力 ");
            add("女神跟我走");
            add("风筝");
            add("无敌县令");
            add("幸福一家人");
            add("大土炮之疯狂导演");
            add("猫和帕普奇");
            add("巴啦啦小魔仙之梦幻旋律");
            add("路从今夜白之遇见青春");
            add("阿甘正传");
            add("大乱斗之少年觉醒");
            add("鬼入侵");
            add("战狼2 ");
            add("圣诞节的12只小狗");
            add("中国农民歌会第2季 ");
            add("可可、可心一家人1 ");
            add("神奇四侠2 ");
            add("摇滚藏獒");
            add("大耳朵图图第4季 ");
            add("熊出没之环球大冒险");
            add("囧探校花");
            add("帮帮龙出动之恐龙探险队 第2季");
            add("拯救悟空");
            add("声临其境");
            add("东北剿匪记");
            add("神兵小将 第2季");
            add("谈判官");
            add("京城81号");
            add("英雄本色1 ");
            add("爸爸的假期");
            add("汽车城之拖车汤姆 第2季");
            add("芝麻胡同");
            add("中华好诗词");
            add("瘦身男女");
            add("何以笙箫默");
            add("巴啦啦小魔仙之奇迹舞步");
            add("三滴血");
            add("蜀山战纪第2季 ");
            add("换了人间");
            add("小熊尼奥之环球旅行");
            add("爱情进化论");
            add("花千骨");
            add("烈火刀影");
            add("哈利·波特7：哈利·波特与死亡圣器(下) ");
            add("帮帮龙出动之恐龙探险队 第2季 花絮篇 ");
            add("光头强儿童故事");
            add("爱情公寓");
            add("银河奥特曼S ");
            add("等你回家");
            add("高斯奥特曼");
            add("粉红女郎之爱人快跑");
            add("米卡成长天地");
            add("那片星空那片海");
            add("反贪风暴3（粤语） ");
            add("西游记之孙悟空三打白骨精");
            add("我的爸爸是森林之王（普通话）");
            add("大牧歌");
            add("重量级改变");
            add("爱情狂想曲");
            add("济公之英雄归位");
            add("枕边有张脸2 ");
            add("天天有喜2之人间有爱 ");
            add("新闺蜜时代");
            add("永远一家人");
            add("郭的秀");
            add("乱世新娘");
            add("太行山上");
            add("亲子游戏");
            add("丁丁鸡玩具故事");
            add("特警屠龙（粤语）");
            add("米奇妙妙车队");
            add("新女婿时代");
            add("哆啦A梦 剧场版");
            add("小鱼儿与花无缺");
            add("记忆碎片");
            add("养母的花样年华");
            add("可可小爱第7季 ");
            add("盗梦空间");
            add("招摇");
            add("暮色大电影");
            add("山海经之赤影传说");
            add("继父回家");
            add("蜜月酒店杀人事件");
            add("新扎师姐");
            add("九条命");
            add("怀特霍尔街上的长统靴");
            add("山谷两日");
            add("稀饭寓言");
            add("妈妈听我说");
            add("超级翁婿");
            add("求求你爱上我");
            add("换脸校花");
            add("恐怖游泳馆");
            add("宝宝巴士拼音汉字");
            add("看见你的声音");
            add("狮子王辛巴");
            add("泡泡孔雀鱼 第1季");
            add("不二神探 粤语 ");
            add("开心乐园幼儿数学算算看(第一季)");
            add("塞尔柯克漂流记");
            add("巴黎危机");
            add("小熊尼奥之梦境小镇");
            add("雷霆使命");
            add("喜剧之王 美国 ");
            add("钟馗捉妖记");
            add("昼颜（普通话）");
            add("极限生存 第一季 ");
            add("熊孩子儿歌KTV ");
            add("死亡派对");
            add("2012");
            add("绝地枪王");
            add("森林总动员 第2季");
            add("托马斯和他的朋友们 第5季 高清版 ");
            add("超能龙骑侠");
            add("放牛班的春天");
            add("头脑特工队");
            add("不朽之名曲");
            add("神兽金刚之天神地兽");
            add("巴塔木特攻队");
            add("猎仇者");
            add("执行利剑");
            add("绝地逢生");
            add("如果爱第3季 ");
            add("熊熊乐园全集");
            add("楚门的世界");
            add("大买卖");
            add("芭比之梦想豪宅 第1季");
            add("终极蜘蛛侠 酷网战士 ");
            add("我的汤姆猫短片");
            add("幼儿亲子教育故事大全");
            add("谋杀似水年华");
            add("鸡排英雄");
            add("灵魂摆渡3 ");
            add("橙红年代");
            add("魔鬼二世");
            add("蓝迪原创舞蹈");
            add("托马斯和他的朋友们之自控力培养");
            add("波儿动起来 中文版 ");
            add("百变马丁");
            add("老爸当家");
            add("幼儿数学启蒙精选");
            add("博物馆奇妙夜");
            add("宝宝巴士启蒙音乐剧");
            add("开心乐园幼儿学英语(第九季)");
            add("冰川时代4 ");
            add("变形金刚：救援机器人");
            add("玩具欢乐秀");
            add("战地枪王");
            add("东北一家人第二部");
            add("《择天记》鹿晗“洗澡”花絮");
            add("大宅男");
            add("北京·夜 ");
            add("钢拳之王");
            add("宝宝巴士拼音汉字");
            add("小猪佩奇 第6季");
            add("笨小孩叶德娴版（粤语）");
            add("小明和他的小伙伴们");
            add("六福喜事 粤语 ");
            add("星际传奇2 ");
            add("蝙蝠侠：黑暗骑士崛起");
            add("魔法俏佳人 第6季");
            add("我的魔姬女友");
            add("蚂蚱");
            add("红色");
            add("少儿舞蹈视频集锦");
            add("鞠萍姐姐讲故事全集");
            add("我的清纯大小姐");
            add("爱故事视频");
            add("热心奇奇");
            add("奇异鸟英语");
            add("小伴龙儿歌");
            add("黑客帝国2：重装上阵 ");
            add("别让爱你的人等太久");
            add("海岛之恋");
            add("旋风战车队 第2季");
            add("八方传奇");
            add("猫小帅圣诞儿歌");
            add("来自星星的继承者们");
            add("诺亚方舟漂流记（普通话）");
            add("小黄人与格鲁日记");
            add("绝地枪王");
            add("邮差");
            add("趣盒子手工课");
            add("黑猫警长之翡翠之星");
            add("宝宝巴士之奇妙汉字 第2季");
            add("废柴赌神");
            add("疯狂的兔子 第1季");
            add("粉墨宝贝");
            add("跟Looi学认知");
            add("复仇者联盟2：奥创纪元（普通话） ");
            add("熊出没之3D拼图");
            add("碟中谍4 ");
            add("灵魂摆渡");
            add("谎话");
            add("熊出没之探险日记2 ");
            add("毒液：致命守护者");
            add("丁丁鸡爱玩具小猪佩奇");
            add("海尔兄弟二十周年纪念版");
            add("远征远征");
            add("怪诞小镇 第1季");
            add("熊出没之探险日记2 ");
            add("激情燃烧的岁月2 ");
            add("儿童英文慢速启蒙儿歌");
            add("我是奋青");
            add("音乐果果星");
            add("未来小子");
            add("偶滴歌神啊第3季 ");
            add("绝杀");
            add("桃花劫");
            add("魔镜(3D)");
            add("厉害了我的叔");
            add("生死朗读");
            add("奇奇探险队");
            add("红高粱");
            add("数码宝贝宇宙 应用怪兽 ");
            add("天才阿公");
            add("赏金猎人");
            add("爱情公寓3 ");
            add("贝瓦儿歌 亲亲妈妈 ");
            add("喜羊羊与灰太狼之竞技大联盟 精选 ");
            add("宝宝巴士儿歌之古诗国学");
            add("克拉之恋");
            add("2017《转星大拜年》春节联欢晚会");
            add("乐高好朋友微短剧 第1季");
            add("少年钢铁侠");
            add("末日崩塌");
            add("颠覆");
            add("开心学园幼儿学习系列");
            add("一代名相陈廷敬");
            add("喜乐长安");
            add("新葫芦娃儿歌");
            add("星猫卡通天地");
            add("我们的少年时代");
            add("儿童成长教育动画");
            add("天上的菊美");
            add("小蜜蜂 第1季");
            add("母语");
            add("太友 第1季");
            add("二胎时代");
            add("芭比之美人鱼历险记 高清版 ");
            add("双面劫匪（普通话）");
            add("大唐玄奘");
            add("追踪章鱼保罗");
            add("同谋");
            add("咩咩小黄羊");
            add("咔咔家族体育系列");
            add("私人订制(极清版)");
            add("极速前进第4季 ");
            add("帕丁顿熊");
            add("超级飞侠 第3季");
            add("逗比僵尸");
            add("错爱2 ");
            add("哎呀妈呀第2季 ");
            add("纸镇");
            add("欢乐好声音");
            add("宝宝巴士之奇妙的节日");
            add("超级飞侠 第3季");
            add("新木乃伊");
            add("魔都凶音");
            add("洛克王国大冒险");
            add("猪猪侠5 ");
            add("下一个球星");
            add("摇滚英雄");
            add("爱探险的朵拉 第7季 英文版 ");
            add("斑马小卓 中文版 ");
            add("致命魔术");
            add("斗龙战士 第2季");
            add("桑园");
            add("格格的贴身医卫");
            add("熊出没之探险日记2 ");
            add("中国家庭");
            add("指环王2：双塔奇兵 ");
            add("汪汪队立大功 第2季");
            add("会说话的迷你家族");
            add("翩翩冷少俏佳人第二季");
            add("我的仨妈俩爸");
            add("RABBYCC之卡罗的心灵世界 ");
            add("我在清朝当密探");
            add("向着幸福前进");
            add("橄榄球联盟");
            add("唐人街探案");
            add("宋小宝小品《爱情不外卖》-2017东方卫视春晚 ");
            add("爸爸请回答");
            add("亮亮和晶晶 短篇版 ");
            add("最美人物");
            add("女总裁的王牌高手");
            add("泰拉星环 精编版 ");
            add("宝宝巴士儿歌之感恩教育");
            add("失恋399年 ");
            add("飞天小女警 第4季");
            add("我的叔叔");
            add("跟着贝尔去冒险");
            add("汽车城之探索冒险篇");
            add("欢乐饭米粒儿第2季 ");
            add("特尔莫和图拉 小手工 ");
            add("2018山西卫视春晚");
            add("滚滚故事学诗词");
            add("无名者传说I：异能觉醒 ");
            add("血伞凶灵");
            add("同趣大调查");
            add("猎魂师");
            add("我的魔镜男友");
            add("美人私房菜");
            add("十全九美之真爱无双");
            add("逆路");
            add("童趣大本营");
            add("飞天少年 第2季");
            add("舞法天女朵法拉");
            add("小猪佩奇之性格培养");
            add("蛋糕哪儿去了 特别篇 ");
            add("春娇救志明");
            add("小鱼儿奇遇记");
            add("激浪青春");
            add("亲宝网儿歌之一起学唱歌");
            add("那年青春正年少");
            add("生逢灿烂的日子");
            add("笑傲江湖2：东方不败 ");
            add("名侦探柯南：章节ONE 变小的名侦探");
            add("神奇卡车");
            add("极限特工3：终极回归 ");
            add("奇异博士");
            add("2016辽宁春晚 宋小宝小品《吃面》 ");
            add("灿烂的恶魔");
            add("夜闯寡妇村");
            add("LooLoo Kids启蒙儿歌 ");
            add("奇趣工坊之科技手工宝典");
            add("小马宝莉 第6季");
            add("傻儿传奇之抗战到底");
            add("轩辕剑之汉之云");
            add("小鼠波波 第2季");
            add("海绵宝宝动画");
            add("破·局 ");
            add("幼儿版百家姓");
            add("欢乐之城 第3季");
            add("卡卡快乐成长");
            add("心理警示录 大赌徒 ");
            add("托马斯系列之逆商教育篇");
            add("进击吧，闪电");
            add("宝宝巴士启蒙音乐剧之动物世界");
            add("欢乐饭米粒儿第4季 ");
            add("巴塔木之儿童英语启蒙");
            add("东方卫视2017跨年演唱会");
            add("你的名字。（普通话）");
            add("侠盗联盟（粤语）");
            add("恐龙宝宝漫游记之我爱我家");
            add("瑞奇宝宝之入园能力");
            add("大爱无言");
            add("疯狂熊孩子");
            add("乐高好朋友微短剧 第1季");
            add("欢乐颂2 ");
            add("北京爱情故事");
            add("辣手回春（粤语）");
            add("传奇大亨");
            add("简装男神");
            add("壁虎盖天王");
            add("贼心魅影");
            add("宝宝巴士儿歌之我爱爸爸");
            add("的士速递5 ");
            add("黑暗之心 下 ");
            add("我的小姨");
            add("美丽的事");
            add("暴走刑警");
            add("仙医神厨3 ");
            add("警中警");
            add("花开那时");
            add("积木宝贝炫酷童谣 第2季");
            add("冰雪奇迹");
            add("海带");
            add("追捕（普通话）");
            add("大梦西游2铁扇公主 ");
            add("【明星甜点】翻糖新创举 北京铜火锅蛋糕 ");
            add("神奇动物");
            add("后羿射日");
            add("FOOD超人");
            add("2015MAMA亚洲音乐盛典全程完整版");
            add("【明星甜点】鸡蛋佐塔塔牛肉慕斯 竟是用这些做 ");
            add("睡衣小英雄");
            add("绑架者");
            add("和迷你卡车学习 第2季");
            add("佣兵的战争");
            add("笑嗷喜剧人");
            add("杨光的快乐生活6 ");
            add("恐龙趣味玩具");
            add("Baby Time ");
            add("一家不说两家话");
            add("兽王争锋 第1季");
            add("积木拼插 玩具 ");
            add("熊出没之探险日记2 ");
            add("超变武兽之王者试炼");
            add("乐享知识乐园 第3季");
            add("嗨 道奇 第2季 ");
            add("暴走拳手");
            add("BabyKidsChannel儿歌 ");
            add("幸福满院");
            add("请爱我的女朋友");
            add("小伶玩具：多人游戏");
            add("喜羊羊与灰太狼之竞技大联盟 精选 ");
            add("绝命刀手之墨玉断");
            add("军武大本营第二季");
            add("玩具大陆 第1季 动漫玩具的世界 ");
            add("闪亮的爸爸第2季 ");
            add("非你莫属");
            add("汪汪队立大功 第2季 英文版 ");
            add("西游记的故事");
            add("小爱的折纸");
            add("那天亨利遇到谁");
            add("深夜食堂");
            add("肖申克的救赎");
            add("谁知道");
            add("幼儿启蒙教育");
            add("乐高幻影忍者番外篇 吴大师的茶铺 英文版");
            add("猫狗 第2季 英文版 ");
            add("无敌小鹿 第2季");
            add("鉴证英雄");
            add("机械娇娃");
            add("妮妮猫儿童故事");
            add("叮当姐姐的玩具口袋");
            add("怒火保镖（普通话）");
            add("香蜜沉沉烬如霜");
            add("最好的安排");
            add("豹宝认识动物");
            add("汪汪队立大功");
            add("小主人之正义联盟");
            add("我的高考我的班");
            add("芭比之梦境奇遇记");
            add("神奇星球");
            add("阿U 第11季 阿U的烦恼");
            add("空天战队");
            add("功夫瑜伽");
            add("今世缘 缘定今生（周间版） ");
            add("积木系列儿童玩具");
            add("玩具開箱｜BomBoo Kids ");
            add("最强大脑之燃烧吧大脑");
            add("至Q宠物屋 第4季 中文版");
            add("遇见大咖第3季 ");
            add("无敌极光侠 英文版 ");
            add("龙门驿站之新嫁衣");
            add("萌鸡小学堂");
            add("熊熊乐园熊出没玩具蛋");
            add("杀破狼·贪狼 ");
            add("52赫兹，我爱你");
            add("托马斯和他的朋友们 第17季 ");
            add("血战湘江（4K）");
            add("007：大破天幕杀机 ");
            add("疯狂创客");
            add("我爱食全食美");
            add("爱斯基摩女孩 第4季");
            add("天狗");
            add("米奇妙妙屋 第4季 中文版 ");
            add("吉屋藏娇");
            add("《偶像练习生》幕后花絮： 尤长胖怀念火锅 与陈立农调皮互侃");
            add("儿童经典儿歌大全");
            add("玩具boy ");
            add("小伶玩具：手工DIY ");
            add("凤囚凰");
            add("夺命轮回");
            add("查理和劳拉 第3季");
            add("月嫂先生");
            add("蓝猫疯狂英语之我要吃西餐");
            add("熊孩子儿歌之劳动最光荣");
            add("阿U 第10季 神奇幻镜 ");
            add("越活越来劲");
            add("史前超人");
            add("艾利亚斯船船总动员");
            add("2019北京卫视春晚");
            add("爱吃鬼乔达 第3季");
            add("农场主的羊驼");
            add("兔小贝安全教育动画 第2季");
            add("狗狗的疯狂假期");
            add("多乐童话");
            add("宝宝巴士儿歌之奇趣大自然");
            add("小阴谋大爱情");
            add("我的珍宝");
            add("通天狄仁杰");
            add("初心");
            add("美少女战士");
            add("老严有女不愁嫁");
            add("解忧杂货店");
            add("你是我兄弟之牌王");
            add("间谍同盟");
            add("东方七色花");
            add("汪汪队立大功 第4季");
            add("凯特与米米兔 第2季");
            add("罗布奥特曼 普通话 ");
            add("一吻定情 泰语版 ");
            add("二十二");
            add("查理和劳拉");
            add("一条狗的使命");
            add("咕力咕力英语说唱");
            add("遇见大咖第4季 ");
            add("小沈阳小品《真的想回家》-2014北京春晚 ");
            add("北京卫视2019跨年演唱会");
            add("大鼻孔玩具故事之精灵宝可梦");
            add("熊出没·变形记 ");
            add("神秘巨星");
            add("龙门驿站之生死阁");
            add("起跑线");
            add("真正的脑洞");
            add("如果我是一只动物");
            add("公主骑士奈拉");
            add("包宿");
            add("宝宝巴士儿歌全集");
            add("回乡偶遇");
            add("无敌小鹿之我爱运动");
            add("火线战警");
            add("追击");
            add("血伞凶灵（4K）");
            add("《声临其境》郭德纲配音《武林外传》白展堂说书");
            add("了不起的孩子第3季 ");
            add("错放你的手");
            add("至暗时刻（普通话版）");
            add("艾拉 奥斯卡和小云 ");
            add("千字文");
            add("终极大冒险");
            add("兔小贝手工课堂 第1季");
            add("帕丁顿熊2 ");
            add("天下寻宝");
            add("凯蒂猫和她的朋友们 一起学习吧 ");
            add("宝宝巴士之奇妙数学大冒险");
            add("我们的四十年——庆祝改革开放40周年文艺晚会");
            add("芭比之梦境奇遇记 英文版 ");
            add("木偶奇遇记");
            add("嘟拉文明礼仪小课堂");
            add("天穹之幻境重生");
            add("巴塔木童谣 第2季");
            add("小伴龙故事");
            add("四世同堂");
            add("王子与公主");
            add("小幻与冲冲 第2季 英文版 ");
            add("艾利亚斯船船总动员 第3季");
            add("临时演员");
            add("芭比之真假公主 高清版 ");
            add("车神");
            add("趣盒子手工课");
            add("萌犬好声音（英文）");
            add("蓝迪原创舞蹈");
            add("梦想合伙人");
            add("太友和啵乐乐英语单词儿歌");
            add("可乐数学");
            add("丁丁在鲨鱼湖");
            add("吃货宇宙");
            add("小公交车太友 第3季上");
            add("小姐与流浪汉");
            add("诺诺森林");
            add("功夫美男");
            add("没说不爱你");
            add("小猪佩奇全集");
            add("汽车城之超级变形卡车 第3季");
            add("凉生我们可不可以不忧伤");
            add("叽里呱啦我爱画画");
            add("果果骑侠传");
            add("你的名字。");
            add("中国梦之声偶像日记");
            add("芝麻街国际版 第47季 ");
            add("疯狂小糖 第5季");
            add("贝瓦儿歌新动力之宝贝宝贝");
            add("中国藏歌会第3季 ");
            add("小云熊儿歌");
            add("星际小蚂蚁之环球追梦 第2季");
            add("天天成长记");
            add("小小演说家第3季 ");
            add("快乐酷宝");
            add("甜筒玩具屋");
            add("神奇马戏团之动物饼干（普通话）");
            add("天线宝宝 第2季");
            add("觅迹追踪");
            add("棒棒的幸福生活");
            add("猪猪侠之竞球小英雄 第2季");
            add("咿呀咿呀之乌糖和他的伙伴们");
            add("桃子简笔画");
            add("疯狂四剑客 第2季 普通话 ");
            add("玛丽与魔女之花（普通话）");
            add("鹰与枭");
            add("将界2 ");
            add("小小大英雄毛毛王");
            add("主妇也要拼");
            add("爱探险的朵拉 第8季");
            add("全球探险冲冲冲 第2季 上 英文版");
            add("51号兵站");
            add("新还珠格格");
            add("疯狂小糖");
            add("芭比之仙子的秘密");
            add("宇宙护卫队");
            add("小猪佩奇之性格培养");
            add("卧底巨星");
            add("巴啦啦小魔仙");
            add("匠心传奇");
            add("镜花水月 第三季 ");
            add("中国好人");
            add("美人鱼(汤姆汉克斯版)");
            add("托马斯玩具");
            add("锋味");
            add("我想象中的朋友");
            add("狂蟒之灾4 ");
            add("工程车汽车玩具DIY ");
            add("饼干警长");
            add("射雕英雄传胡歌版");
            add("碰碰狐 英语公主童话 ");
            add("儿童国学精选");
            add("最美的青春");
            add("逗逗迪迪之美梦精灵");
            add("阿U学科学 第2季 ");
            add("碰碰狐之圣诞儿歌");
            add("无敌小鹿 家庭儿歌 ");
            add("方块熊简笔画");
            add("黑暗迷宫");
            add("绿巨人");
            add("奇趣工坊之魔法气球宝典");
            add("美国劫案");
            add("猎毒人");
            add("欢迎来韦恩");
            add("小猪佩奇 第5季");
            add("猫狗");
            add("天才小琴童");
            add("新乌龙院之笑闹江湖");
            add("小龙人奇遇记 第二季 ");
            add("我是一只鱼 英文版 ");
            add("欢天戏地");
            add("绝代艳后 2006年 ");
            add("小羊肖恩冠军羊全集");
            add("玛利亚的车学堂");
            add("冒险小勇士 第2季");
            add("哆啦A梦：大雄的金银岛 ");
            add("少年派的奇幻漂流");
            add("朗读者第2季 ");
            add("欧布奥特曼 普通话版 ");
            add("碰碰狐 动物儿歌 第2季 ");
            add("小猪佩奇 英文版 ");
            add("坑王驾到第2季 ");
            add("忠爱无言（4K）");
            add("欢乐元帅");
            add("爱探险的朵拉 第8季");
            add("禁海苍狼");
            add("神笔扎克 普通话 ");
            add("马上天下");
            add("我的冤家是条狗");
            add("碰碰狐之伊索寓言故事 第2季");
            add("淮南子传奇");
            add("立场");
            add("私语岛");
            add("超能玩具白白侠");
            add("丹尼尔和邻居们 第3季 英文版 ");
            add("老梁体育评书");
            add("萌宝找辣妈");
            add("LOVE");
            add("小龙大功夫 第3季");
            add("西游超级粉");
            add("小鱼人莫叽姆斯一家");
            add("捷德奥特曼 普通话版 ");
            add("神笔扎克 普通话 ");
            add("独孤皇后");
            add("芭比之歌星公主");
            add("大丫鬟");
            add("西京故事");
            add("方块熊乐园");
            add("音乐熊猫儿歌");
            add("星选者联盟 第2季 龙家的操控 ");
            add("昼颜（普通话）");
            add("阿甘妙世界");
            add("关关雎鸠");
            add("加勒比海盗4(3D) ");
            add("贝瓦儿歌之寓言儿歌");
            add("阿凡提的故事");
            add("草原豆思 第3季");
            add("小马宝莉");
            add("简爱之约");
            add("不服来战");
            add("来吧伽利略");
            add("吉林卫视2019跨年晚会");
            add("嘟嘟小仓鼠");
            add("奇葩说不停");
            add("神风刀");
            add("国家宝藏片场纪事");
            add("中国神探");
            add("学习颜色 学习形状 ");
            add("好习惯不用教");
            add("怦然星动");
            add("独孤皇后");
            add("洛克王国大冒险");
            add("反贪风暴");
            add("嫁给爱情");
            add("情敌复仇战");
            add("石头娃");
            add("千千简笔画之十二生肖传统文化");
            add("熊出没之探险日记");
            add("像我们一样年轻");
            add("火力少年王3 ");
            add("2012（4K）");
            add("警察故事续集");
            add("还是夫妻");
            add("胡桃夹子和四个王国（普通话）");
            add("延安锄奸");
            add("王者归来");
            add("丁丁历险记之太阳的囚徒");
            add("追求幸福的日子");
            add("哈罗，UFO ");
            add("嘟嘟小巴士之数学城堡");
            add("乡约节目完整版");
            add("医妃难囚");
            add("盖亚奥特曼");
            add("狄仁杰之通天帝国（4K）");
            add("海底小纵队 精编版 ");
            add("小鬼当家3 ");
            add("教父");
            add("22年后的自白");
            add("故事枕头");
            add("亲子手工");
            add("大侦探");
            add("超能泰坦");
            add("最后的晚餐");
            add("精忠岳飞");
            add("老小阿凡提");
            add("猪猪侠之竞球小英雄");
            add("绿树林家族 第2季");
            add("特警屠龙（粤语）");
            add("无敌鼠宝宝（长篇）");
            add("可爱的骨头");
            add("听得到的美食第2季 ");
            add("雪域天路");
            add("生活启示录");
            add("海克星");
            add("芸汐传");
            add("启动幸福");
            add("龙之谷：破晓奇兵");
            add("憨哥进城");
            add("风花仙子外传");
            add("亡命救赎");
            add("口袋里的宠物");
            add("百变五侠之我是大明星");
            add("警察游戏");
            add("女人如花");
            add("陌路杀机");
            add("心理罪（4K）");
            add("四个春天");
            add("男人不可以穷 粤语 ");
            add("企鹅岛");
            add("大象国王巴巴 英文版 ");
            add("你是我的生命");
            add("猫小帅十万个为什么");
            add("全面围攻");
            add("白雪公主之矮人力量");
            add("新葫芦兄弟 第1季 上篇 ");
            add("熊出没");
            add("火锅英雄");
            add("青蛙王子之蛙蛙探险队");
            add("小飞机卡卡");
            add("小花仙");
            add("妈妈咪呀2015");
            add("猩球征服");
            add("神奇遥控器");
            add("越空追击");
            add("宝乐学习记一起数一数 英文版 ");
            add("跨界喜剧王第3季 ");
            add("东东动画系列之三字经");
            add("海底小英雄第一季");
            add("星际迷航4：抢救未来 ");
            add("小飞机卡卡之直升机芒果文");
            add("超能陆战队 粤语 ");
            add("良心");
            add("蓝猫儿歌KTV ");
            add("刺客与保镖");
            add("盗墓笔记");
            add("追爱大布局");
            add("异次元骇客");
            add("下辈子做你的女人");
            add("罗布奥特曼");
            add("小黄人与格鲁日记");
            add("小马哥民间故事");
            add("可可小爱童谣第一季");
            add("那件疯狂的小事叫爱情");
            add("奇幻精灵事件簿");
            add("幼儿版三字经");
            add("机械公敌");
            add("跳跳鱼世界全集");
            add("周恩来的四个昼夜");
            add("赛尔号第4季之战神风云决 ");
            add("爱探险的朵拉");
            add("变形金刚之雷霆舰队");
            add("延禧攻略");
            add("黑洞");
            add("华尔街");
            add("能耐大了");
            add("小公主苏菲亚 第4季");
            add("魔法老师");
            add("蓝猫淘气3000问之平安出行");
            add("银河奥特曼S ");
            add("鹅毛笔");
            add("红龙");
            add("使徒行者");
            add("小布与伟仔 第1季");
            add("我是你爸爸");
            add("垫底辣妹");
            add("士兵突击");
            add("圈套剧场版4：最后的舞台 ");
            add("波西·杰克逊与神火之盗 ");
            add("俄罗斯方舟");
            add("奇思妙世界");
            add("小骑士迈克 电影版 神龙大山历险记");
            add("鼠小弟亚瑟 中文版 ");
            add("超人高校");
            add("乌士儿和平平之奇迹花园探索冒险记");
            add("女神捕");
            add("和你一起飞");
            add("琪琪的秘密日记");
            add("萌宝读古诗");
            add("声调小方块点心");
            add("神探包星星第2季 ");
            add("洪吉童2084");
            add("唐颂教育之唐诗精选");
            add("歌舞青春");
            add("发酵吧，创业菌");
            add("新恋爱时代");
            add("智取威虎山");
            add("外来媳妇本地郎第二部");
            add("超能宝贝");
            add("萌宠特工队之致命病毒");
            add("大唐情史");
            add("小布与伟仔 第2季");
            add("聪明伦文叙 第2季");
            add("天天有喜");
            add("梦想星搭档第2季 ");
            add("一步之遥（4K）");
            add("上新了故宫");
            add("夺帅");
            add("杨三姐告状");
            add("香草天空");
            add("目击者");
            add("哆啦A梦：伴我同行(3D) ");
            add("乐比悠悠科普系列之动物自然");
            add("遭诅咒的村庄");
            add("宝狄英文儿歌 第2季");
            add("丛林之书 英文版 ");
            add("床下有人2 ");
            add("地狱男爵2：黄金军团 ");
            add("二十四周（4K）");
            add("死海");
            add("巨神战击队2 ");
            add("我的老婆大人是八零后");
            add("小乌龟富兰克林又开学了 英文版 ");
            add("铁腕行动");
            add("变形警车珀利 交通安全篇 英文版");
            add("美女与野兽动画版");
            add("寻龙诀");
            add("恰克大冒险 第二季 ");
            add("勇闯禁地");
            add("星星的礼物第3季 ");
            add("奔（4K）");
            add("五鼠闹东京");
            add("YOYO MAN 第2季");
            add("白幽灵传奇之绝命逃亡");
            add("麦酷狮魔法历险记2 英语版");
            add("龙凤配");
            add("雾都魅影");
            add("无法触碰的爱（4K）");
            add("完美婚礼");
            add("碟中谍");
            add("如烟旧事");
            add("巨额交易");
            add("星动亚洲第二季");
            add("布鲁精灵 第3季");
            add("新特警判官(3D)");
            add("七十七天（4K）");
            add("泰坦尼克号");
            add("我的老婆是只猫");
        }};

        Random rd=new Random();
        Integer num= rd.nextInt(list.size());
       return list.get(num);

    }

    // 创建映射,dynamic三个参数：true、false、strict
    //true : 将新识别的字段加入mapping (默认选项)
    //false: 新识别的字段被忽略，不会被添加到mapping中，新字段必须被显式的添加到mapping中。 这些字段不会被索引也不能被搜索，但是仍然会出现在_source字段中。
    //strict: 有新字段时会抛出异常，拒绝文档写入。新字段必须显式的添加到mapping中。
    public static void createMapping(String INDEX) throws Exception{
        TransportClient client= getClient();
        String TYPE = "_doc";
        // 配置映射关系
        Map<String,Object> mappings = new HashMap<>();
        Map<String,Object> type = new HashMap<>();
        mappings.put(TYPE, type);
        type.put("dynamic", "true");

        Map<String,Object> properties = new HashMap<>();
        type.put("properties", properties);

        // 文档的id映射
        Map<String,Object> idProperties = new HashMap<>();
        idProperties.put("type", "keyword");
       // idProperties.put("store", "true");
        idProperties.put("index", "true");
        properties.put("mac", idProperties);

        // 文档的title映射
        Map<String,Object> titleProperties = new HashMap<>();
        titleProperties.put("type", "keyword");
       // titleProperties.put("store", "true");
        titleProperties.put("index", "true");
        properties.put("partner", titleProperties);

        // 文档的title映射
        Map<String,Object> nameProperties = new HashMap<>();
        nameProperties.put("type", "long");
       // nameProperties.put("store", "true");
        nameProperties.put("index", "true");
        properties.put("albumId", nameProperties);

        // 文档的title映射
        Map<String,Object> albumProperties = new HashMap<>();
        albumProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        albumProperties.put("index", "true");
        properties.put("albumName", albumProperties);


        Gson gson2 = new GsonBuilder().enableComplexMapKeySerialization().create();
        String json = gson2.toJson(mappings);
        System.out.println(json);

        PutMappingRequest request = Requests.putMappingRequest(INDEX).type(TYPE).source(json, XContentType.JSON);
        client.admin().indices().putMapping(request).actionGet();

    }



    public static void createDataMapping10(String INDEX) throws Exception{
        TransportClient client= getClient();
        String TYPE = "_doc";
        // 配置映射关系
        Map<String,Object> mappings = new HashMap<>();
        Map<String,Object> type = new HashMap<>();
        mappings.put(TYPE, type);
        type.put("dynamic", "true");

        Map<String,Object> properties = new HashMap<>();
        type.put("properties", properties);

        // 文档的id映射
        Map<String,Object> idProperties = new HashMap<>();
        idProperties.put("type", "keyword");
        // idProperties.put("store", "true");
        idProperties.put("index", "true");
        properties.put("mac", idProperties);

        // 文档的title映射
        Map<String,Object> titleProperties = new HashMap<>();
        titleProperties.put("type", "keyword");
        // titleProperties.put("store", "true");
        titleProperties.put("index", "true");
        properties.put("partner", titleProperties);

        // 文档的title映射
        Map<String,Object> nameProperties = new HashMap<>();
        nameProperties.put("type", "long");
        // nameProperties.put("store", "true");
        nameProperties.put("index", "true");
        properties.put("albumId", nameProperties);

        // 文档的title映射
        Map<String,Object> albumProperties = new HashMap<>();
        albumProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        albumProperties.put("index", "true");
        properties.put("albumName", albumProperties);

        Map<String,Object> dayProperties = new HashMap<>();
        dayProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        dayProperties.put("index", "true");
        properties.put("day", dayProperties);

        Map<String,Object> actionProperties = new HashMap<>();
        actionProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        actionProperties.put("index", "true");
        properties.put("action", actionProperties);

        Map<String,Object> logIdProperties = new HashMap<>();
        logIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        logIdProperties.put("index", "true");
        properties.put("logId", logIdProperties);

        Map<String,Object> productIdProperties = new HashMap<>();
        productIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        productIdProperties.put("index", "true");
        properties.put("productId", productIdProperties);

        Map<String,Object> ipProperties = new HashMap<>();
        ipProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        ipProperties.put("index", "true");
        properties.put("ip", ipProperties);

        Map<String,Object> pingbackVersionProperties = new HashMap<>();
        pingbackVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pingbackVersionProperties.put("index", "true");
        properties.put("pingbackVersion", pingbackVersionProperties);

        Map<String,Object> clientVersionProperties = new HashMap<>();
        clientVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientVersionProperties.put("index", "true");
        properties.put("clientVersion", clientVersionProperties);

        Map<String,Object> appMarkProperties = new HashMap<>();
        appMarkProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        appMarkProperties.put("index", "true");
        properties.put("appMark", appMarkProperties);

        Map<String,Object> clientTimeProperties = new HashMap<>();
        clientTimeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientTimeProperties.put("index", "true");
        properties.put("clientTime", clientTimeProperties);
    }

    public static void createDataMapping20(String INDEX) throws Exception{
        TransportClient client= getClient();
        String TYPE = "_doc";
        // 配置映射关系
        Map<String,Object> mappings = new HashMap<>();
        Map<String,Object> type = new HashMap<>();
        mappings.put(TYPE, type);
        type.put("dynamic", "true");

        Map<String,Object> properties = new HashMap<>();
        type.put("properties", properties);

        // 文档的id映射
        Map<String,Object> idProperties = new HashMap<>();
        idProperties.put("type", "keyword");
        // idProperties.put("store", "true");
        idProperties.put("index", "true");
        properties.put("mac", idProperties);

        // 文档的title映射
        Map<String,Object> titleProperties = new HashMap<>();
        titleProperties.put("type", "keyword");
        // titleProperties.put("store", "true");
        titleProperties.put("index", "true");
        properties.put("partner", titleProperties);

        // 文档的title映射
        Map<String,Object> nameProperties = new HashMap<>();
        nameProperties.put("type", "long");
        // nameProperties.put("store", "true");
        nameProperties.put("index", "true");
        properties.put("albumId", nameProperties);

        // 文档的title映射
        Map<String,Object> albumProperties = new HashMap<>();
        albumProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        albumProperties.put("index", "true");
        properties.put("albumName", albumProperties);

        Map<String,Object> dayProperties = new HashMap<>();
        dayProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        dayProperties.put("index", "true");
        properties.put("day", dayProperties);

        Map<String,Object> actionProperties = new HashMap<>();
        actionProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        actionProperties.put("index", "true");
        properties.put("action", actionProperties);

        Map<String,Object> logIdProperties = new HashMap<>();
        logIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        logIdProperties.put("index", "true");
        properties.put("logId", logIdProperties);

        Map<String,Object> productIdProperties = new HashMap<>();
        productIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        productIdProperties.put("index", "true");
        properties.put("productId", productIdProperties);

        Map<String,Object> ipProperties = new HashMap<>();
        ipProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        ipProperties.put("index", "true");
        properties.put("ip", ipProperties);

        Map<String,Object> pingbackVersionProperties = new HashMap<>();
        pingbackVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pingbackVersionProperties.put("index", "true");
        properties.put("pingbackVersion", pingbackVersionProperties);

        Map<String,Object> clientVersionProperties = new HashMap<>();
        clientVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientVersionProperties.put("index", "true");
        properties.put("clientVersion", clientVersionProperties);

        Map<String,Object> appMarkProperties = new HashMap<>();
        appMarkProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        appMarkProperties.put("index", "true");
        properties.put("appMark", appMarkProperties);

        Map<String,Object> clientTimeProperties = new HashMap<>();
        clientTimeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientTimeProperties.put("index", "true");
        properties.put("clientTime", clientTimeProperties);

        Map<String,Object> timeProperties = new HashMap<>();
        timeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        timeProperties.put("index", "true");
        properties.put("time", timeProperties);

        Map<String,Object> logDateProperties = new HashMap<>();
        logDateProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        logDateProperties.put("index", "true");
        properties.put("logDate", logDateProperties);

        Map<String,Object> videoIdProperties = new HashMap<>();
        videoIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoIdProperties.put("index", "true");
        properties.put("videoId", videoIdProperties);

        Map<String,Object> dataRateIdProperties = new HashMap<>();
        dataRateIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        dataRateIdProperties.put("index", "true");
        properties.put("dataRateId", dataRateIdProperties);

        Map<String,Object> videoDurationProperties = new HashMap<>();
        videoDurationProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoDurationProperties.put("index", "true");
        properties.put("videoDuration", videoDurationProperties);

        Map<String,Object> channelIdProperties = new HashMap<>();
        channelIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        channelIdProperties.put("index", "true");
        properties.put("channelId", channelIdProperties);

        Map<String,Object> actionSrcProperties = new HashMap<>();
        actionSrcProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        actionSrcProperties.put("index", "true");
        properties.put("actionSrc", actionSrcProperties);

        Map<String,Object> actionSubSrcNameProperties = new HashMap<>();
        actionSubSrcNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        actionSubSrcNameProperties.put("index", "true");
        properties.put("actionSubSrcName", actionSubSrcNameProperties);

        Map<String,Object> contentSrcIdProperties = new HashMap<>();
        contentSrcIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        contentSrcIdProperties.put("index", "true");
        properties.put("contentSrcId", contentSrcIdProperties);

        Map<String,Object> videoSrcIdProperties = new HashMap<>();
        videoSrcIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoSrcIdProperties.put("index", "true");
        properties.put("videoSrcId", videoSrcIdProperties);

        Map<String,Object> videoIndexProperties = new HashMap<>();
        videoIndexProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoIndexProperties.put("index", "true");
        properties.put("videoIndex", videoIndexProperties);

        Map<String,Object> videoTypeProperties = new HashMap<>();
        videoTypeProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoTypeProperties.put("index", "true");
        properties.put("videoType", videoTypeProperties);

        Map<String,Object> topicIdProperties = new HashMap<>();
        topicIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        topicIdProperties.put("index", "true");
        properties.put("topicId", topicIdProperties);

        Gson gson2 = new GsonBuilder().enableComplexMapKeySerialization().create();
        String json = gson2.toJson(mappings);
        System.out.println(json);

        PutMappingRequest request = Requests.putMappingRequest(INDEX).type(TYPE).source(json, XContentType.JSON);
        client.admin().indices().putMapping(request).actionGet();

    }


    public static void createDataMapping(String INDEX) throws Exception{
        TransportClient client= getClient();
        String TYPE = "_doc";
        // 配置映射关系
        Map<String,Object> mappings = new HashMap<>();
        Map<String,Object> type = new HashMap<>();
        mappings.put(TYPE, type);
        type.put("dynamic", "true");

        Map<String,Object> properties = new HashMap<>();
        type.put("properties", properties);

        // 文档的id映射
        Map<String,Object> idProperties = new HashMap<>();
        idProperties.put("type", "keyword");
        // idProperties.put("store", "true");
        idProperties.put("index", "true");
        properties.put("mac", idProperties);

        // 文档的title映射
        Map<String,Object> titleProperties = new HashMap<>();
        titleProperties.put("type", "keyword");
        // titleProperties.put("store", "true");
        titleProperties.put("index", "true");
        properties.put("partner", titleProperties);

        // 文档的title映射
        Map<String,Object> nameProperties = new HashMap<>();
        nameProperties.put("type", "long");
        // nameProperties.put("store", "true");
        nameProperties.put("index", "true");
        properties.put("albumId", nameProperties);

        // 文档的title映射
        Map<String,Object> albumProperties = new HashMap<>();
        albumProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        albumProperties.put("index", "true");
        properties.put("albumName", albumProperties);

        Map<String,Object> dayProperties = new HashMap<>();
        dayProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        dayProperties.put("index", "true");
        properties.put("day", dayProperties);

        Map<String,Object> actionProperties = new HashMap<>();
        actionProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        actionProperties.put("index", "true");
        properties.put("action", actionProperties);

        Map<String,Object> logIdProperties = new HashMap<>();
        logIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        logIdProperties.put("index", "true");
        properties.put("logId", logIdProperties);

        Map<String,Object> productIdProperties = new HashMap<>();
        productIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        productIdProperties.put("index", "true");
        properties.put("productId", productIdProperties);

        Map<String,Object> ipProperties = new HashMap<>();
        ipProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        ipProperties.put("index", "true");
        properties.put("ip", ipProperties);

        Map<String,Object> pingbackVersionProperties = new HashMap<>();
        pingbackVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pingbackVersionProperties.put("index", "true");
        properties.put("pingbackVersion", pingbackVersionProperties);

        Map<String,Object> clientVersionProperties = new HashMap<>();
        clientVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientVersionProperties.put("index", "true");
        properties.put("clientVersion", clientVersionProperties);

        Map<String,Object> appMarkProperties = new HashMap<>();
        appMarkProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        appMarkProperties.put("index", "true");
        properties.put("appMark", appMarkProperties);

        Map<String,Object> clientTimeProperties = new HashMap<>();
        clientTimeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        clientTimeProperties.put("index", "true");
        properties.put("clientTime", clientTimeProperties);

      /*  Map<String,Object> timeProperties = new HashMap<>();
        timeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        timeProperties.put("index", "true");
        properties.put("time", timeProperties);

        Map<String,Object> logDateProperties = new HashMap<>();
        logDateProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        logDateProperties.put("index", "true");
        properties.put("logDate", logDateProperties);

        Map<String,Object> videoIdProperties = new HashMap<>();
        videoIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoIdProperties.put("index", "true");
        properties.put("videoId", videoIdProperties);

        Map<String,Object> dataRateIdProperties = new HashMap<>();
        dataRateIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        dataRateIdProperties.put("index", "true");
        properties.put("dataRateId", dataRateIdProperties);

        Map<String,Object> videoDurationProperties = new HashMap<>();
        videoDurationProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoDurationProperties.put("index", "true");
        properties.put("videoDuration", videoDurationProperties);

        Map<String,Object> channelIdProperties = new HashMap<>();
        channelIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        channelIdProperties.put("index", "true");
        properties.put("channelId", channelIdProperties);

        Map<String,Object> actionSrcProperties = new HashMap<>();
        actionSrcProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        actionSrcProperties.put("index", "true");
        properties.put("actionSrc", actionSrcProperties);

        Map<String,Object> actionSubSrcNameProperties = new HashMap<>();
        actionSubSrcNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        actionSubSrcNameProperties.put("index", "true");
        properties.put("actionSubSrcName", actionSubSrcNameProperties);

        Map<String,Object> contentSrcIdProperties = new HashMap<>();
        contentSrcIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        contentSrcIdProperties.put("index", "true");
        properties.put("contentSrcId", contentSrcIdProperties);

        Map<String,Object> videoSrcIdProperties = new HashMap<>();
        videoSrcIdProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoSrcIdProperties.put("index", "true");
        properties.put("videoSrcId", videoSrcIdProperties);

        Map<String,Object> videoIndexProperties = new HashMap<>();
        videoIndexProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoIndexProperties.put("index", "true");
        properties.put("videoIndex", videoIndexProperties);

        Map<String,Object> videoTypeProperties = new HashMap<>();
        videoTypeProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        videoTypeProperties.put("index", "true");
        properties.put("videoType", videoTypeProperties);

        Map<String,Object> topicIdProperties = new HashMap<>();
        topicIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        topicIdProperties.put("index", "true");
        properties.put("topicId", topicIdProperties);


        Map<String,Object> topicNameProperties = new HashMap<>();
        topicNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        topicNameProperties.put("index", "true");
        properties.put("topicName", topicNameProperties);

        Map<String,Object> positionProperties = new HashMap<>();
        positionProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        positionProperties.put("index", "true");
        properties.put("position", positionProperties);


        Map<String,Object> cdnProperties = new HashMap<>();
        cdnProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        cdnProperties.put("index", "true");
        properties.put("cdn", cdnProperties);

        Map<String,Object> playOffsetProperties = new HashMap<>();
        playOffsetProperties.put("type", "long");
        //albumProperties.put("store", "true");
        playOffsetProperties.put("index", "true");
        properties.put("playOffset", playOffsetProperties);

        Map<String,Object> xProperties = new HashMap<>();
        xProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        xProperties.put("index", "true");
        properties.put("x", xProperties);

        Map<String,Object> yProperties = new HashMap<>();
        yProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        yProperties.put("index", "true");
        properties.put("y", yProperties);

        Map<String,Object> videoSrcCdnProperties = new HashMap<>();
        videoSrcCdnProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoSrcCdnProperties.put("index", "true");
        properties.put("videoSrcCdn", videoSrcCdnProperties);

        Map<String,Object> userAgentProperties = new HashMap<>();
        userAgentProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        userAgentProperties.put("index", "true");
        properties.put("userAgent", userAgentProperties);

        Map<String,Object> playDurationProperties = new HashMap<>();
        playDurationProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        playDurationProperties.put("index", "true");
        properties.put("playDuration", playDurationProperties);

        Map<String,Object> clientPlayDurationProperties = new HashMap<>();
        clientPlayDurationProperties.put("type", "integer");
        //albumProperties.put("store", "true");
        clientPlayDurationProperties.put("index", "true");
        properties.put("clientPlayDuration", clientPlayDurationProperties);

        Map<String,Object> userIdProperties = new HashMap<>();
        userIdProperties.put("type", "long");
        //albumProperties.put("store", "true");
        userIdProperties.put("index", "true");
        properties.put("userId", userIdProperties);

        Map<String,Object> resolutionProperties = new HashMap<>();
        resolutionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        resolutionProperties.put("index", "true");
        properties.put("resolution", resolutionProperties);

        Map<String,Object> videoNameProperties = new HashMap<>();
        videoNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoNameProperties.put("index", "true");
        properties.put("videoName", videoNameProperties);


        Map<String,Object> continuePlayProperties = new HashMap<>();
        continuePlayProperties.put("type", "long");
        //albumProperties.put("store", "true");
        continuePlayProperties.put("index", "true");
        properties.put("continuePlay", continuePlayProperties);

        Map<String,Object> processStartTimeProperties = new HashMap<>();
        processStartTimeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        processStartTimeProperties.put("index", "true");
        properties.put("processStartTime", processStartTimeProperties);


        Map<String,Object> processEndTimeProperties = new HashMap<>();
        processEndTimeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        processEndTimeProperties.put("index", "true");
        properties.put("processEndTime", processEndTimeProperties);

        Map<String,Object> osVersionProperties = new HashMap<>();
        osVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        osVersionProperties.put("index", "true");
        properties.put("osVersion", osVersionProperties);

        Map<String,Object> srankingIdProperties = new HashMap<>();
        srankingIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        srankingIdProperties.put("index", "true");
        properties.put("srankingId", srankingIdProperties);

        Map<String,Object> srankingNameProperties = new HashMap<>();
        srankingNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        srankingNameProperties.put("index", "true");
        properties.put("srankingName", srankingNameProperties);

        Map<String,Object> playTypeProperties = new HashMap<>();
        playTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        playTypeProperties.put("index", "true");
        properties.put("playType", playTypeProperties);

        Map<String,Object> bufferTypeProperties = new HashMap<>();
        bufferTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        bufferTypeProperties.put("index", "true");
        properties.put("bufferType", bufferTypeProperties);

        Map<String,Object> pauseDurationProperties = new HashMap<>();
        pauseDurationProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pauseDurationProperties.put("index", "true");
        properties.put("pauseDuration", pauseDurationProperties);

        Map<String,Object> bufferDurationProperties = new HashMap<>();
        bufferDurationProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        bufferDurationProperties.put("index", "true");
        properties.put("bufferDuration", bufferDurationProperties);

        Map<String,Object> lsProperties = new HashMap<>();
        lsProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        lsProperties.put("index", "true");
        properties.put("ls", lsProperties);

        Map<String,Object> repairProperties = new HashMap<>();
        repairProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        repairProperties.put("index", "true");
        properties.put("repair", repairProperties);

        Map<String,Object> serialNumberProperties = new HashMap<>();
        serialNumberProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        serialNumberProperties.put("index", "true");
        properties.put("serialNumber", serialNumberProperties);

        Map<String,Object> searchKeyProperties = new HashMap<>();
        searchKeyProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        searchKeyProperties.put("index", "true");
        properties.put("searchKey", searchKeyProperties);

        Map<String,Object> searchTypeProperties = new HashMap<>();
        searchTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        searchTypeProperties.put("index", "true");
        properties.put("searchType", searchTypeProperties);

        Map<String,Object> tagIdProperties = new HashMap<>();
        tagIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        tagIdProperties.put("index", "true");
        properties.put("tagId", tagIdProperties);

        Map<String,Object> tagNameProperties = new HashMap<>();
        tagNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        tagNameProperties.put("index", "true");
        properties.put("tagName", tagNameProperties);

        Map<String,Object> orderOptTypeProperties = new HashMap<>();
        orderOptTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        orderOptTypeProperties.put("index", "true");
        properties.put("orderOptType", orderOptTypeProperties);

        Map<String,Object> prodPackageIdProperties = new HashMap<>();
        prodPackageIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        prodPackageIdProperties.put("index", "true");
        properties.put("prodPackageId", prodPackageIdProperties);

        Map<String,Object> payTypeProperties = new HashMap<>();
        payTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        payTypeProperties.put("index", "true");
        properties.put("payType", payTypeProperties);

        Map<String,Object> payAccountProperties = new HashMap<>();
        payAccountProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        payAccountProperties.put("index", "true");
        properties.put("payAccount", payAccountProperties);

        Map<String,Object> resultProperties = new HashMap<>();
        resultProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        resultProperties.put("index", "true");
        properties.put("result", resultProperties);

        Map<String,Object> payPositionProperties = new HashMap<>();
        payPositionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        payPositionProperties.put("index", "true");
        properties.put("payPosition", payPositionProperties);

        Map<String,Object> videoPriceProperties = new HashMap<>();
        videoPriceProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        videoPriceProperties.put("index", "true");
        properties.put("videoPrice", videoPriceProperties);

        Map<String,Object> actorNameProperties = new HashMap<>();
        actorNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        actorNameProperties.put("index", "true");
        properties.put("actorName", actorNameProperties);

        Map<String,Object> actorIdProperties = new HashMap<>();
        actorIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        actorIdProperties.put("index", "true");
        properties.put("actorId", actorIdProperties);

        Map<String,Object> recommendActTypeProperties = new HashMap<>();
        recommendActTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        recommendActTypeProperties.put("index", "true");
        properties.put("recommendActType", recommendActTypeProperties);

        Map<String,Object> upgradeModeProperties = new HashMap<>();
        upgradeModeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        upgradeModeProperties.put("index", "true");
        properties.put("upgradeMode", upgradeModeProperties);

        Map<String,Object> lastVersionProperties = new HashMap<>();
        lastVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        lastVersionProperties.put("index", "true");
        properties.put("lastVersion", lastVersionProperties);

        Map<String,Object> searchHitProperties = new HashMap<>();
        searchHitProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        searchHitProperties.put("index", "true");
        properties.put("searchHit", searchHitProperties);

        Map<String,Object> wallpaperProperties = new HashMap<>();
        wallpaperProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        wallpaperProperties.put("index", "true");
        properties.put("wallpaper", wallpaperProperties);

        Map<String,Object> keyboardTypeProperties = new HashMap<>();
        keyboardTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        keyboardTypeProperties.put("index", "true");
        properties.put("keyboardType", keyboardTypeProperties);

        Map<String,Object> modelIdProperties = new HashMap<>();
        modelIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        modelIdProperties.put("index", "true");
        properties.put("modelId", modelIdProperties);

        Map<String,Object> modelNameProperties = new HashMap<>();
        modelNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        modelNameProperties.put("index", "true");
        properties.put("modelName", modelNameProperties);

        Map<String,Object> exposedTagIdProperties = new HashMap<>();
        exposedTagIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        exposedTagIdProperties.put("index", "true");
        properties.put("exposedTagId", exposedTagIdProperties);

        Map<String,Object> oldVersionProperties = new HashMap<>();
        oldVersionProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        oldVersionProperties.put("index", "true");
        properties.put("oldVersion", oldVersionProperties);

        Map<String,Object> bandwidthProperties = new HashMap<>();
        bandwidthProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        bandwidthProperties.put("index", "true");
        properties.put("bandwidth", bandwidthProperties);

        Map<String,Object> favorTypeProperties = new HashMap<>();
        favorTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        favorTypeProperties.put("index", "true");
        properties.put("favorType", favorTypeProperties);

        Map<String,Object> rankingIdProperties = new HashMap<>();
        rankingIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        rankingIdProperties.put("index", "true");
        properties.put("rankingId", rankingIdProperties);

        Map<String,Object> rankingNameProperties = new HashMap<>();
        rankingNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        rankingNameProperties.put("index", "true");
        properties.put("rankingName", rankingNameProperties);

        Map<String,Object> voiceCommandProperties = new HashMap<>();
        voiceCommandProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        voiceCommandProperties.put("index", "true");
        properties.put("voiceCommand", voiceCommandProperties);

        Map<String,Object> searchModeProperties = new HashMap<>();
        searchModeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        searchModeProperties.put("index", "true");
        properties.put("searchMode", searchModeProperties);

        Map<String,Object> favorModeProperties = new HashMap<>();
        favorModeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        favorModeProperties.put("index", "true");
        properties.put("favorMode", favorModeProperties);

        Map<String,Object> h5vProperties = new HashMap<>();
        h5vProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        h5vProperties.put("index", "true");
        properties.put("h5v", h5vProperties);

        Map<String,Object> pageIdProperties = new HashMap<>();
        pageIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pageIdProperties.put("index", "true");
        properties.put("pageId", pageIdProperties);

        Map<String,Object> pageNameProperties = new HashMap<>();
        pageNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pageNameProperties.put("index", "true");
        properties.put("pageName", pageNameProperties);

        Map<String,Object> pageTypeProperties = new HashMap<>();
        pageTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        pageTypeProperties.put("index", "true");
        properties.put("pageType", pageTypeProperties);

        Map<String,Object> entryTypeProperties = new HashMap<>();
        entryTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        entryTypeProperties.put("index", "true");
        properties.put("entryType", entryTypeProperties);

        Map<String,Object> termTypeProperties = new HashMap<>();
        termTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        termTypeProperties.put("index", "true");
        properties.put("termType", termTypeProperties);

        Map<String,Object> h5OptIdProperties = new HashMap<>();
        h5OptIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        h5OptIdProperties.put("index", "true");
        properties.put("termType", h5OptIdProperties);

        Map<String,Object> h5OptNameProperties = new HashMap<>();
        h5OptNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        h5OptNameProperties.put("index", "true");
        properties.put("h5OptName", h5OptNameProperties);

        Map<String,Object> quaProperties = new HashMap<>();
        quaProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        quaProperties.put("index", "true");
        properties.put("qua", quaProperties);

        Map<String,Object> winProperties = new HashMap<>();
        winProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        winProperties.put("index", "true");
        properties.put("win", winProperties);

        Map<String,Object> contentidProperties = new HashMap<>();
        contentidProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        contentidProperties.put("index", "true");
        properties.put("contentid", contentidProperties);

        Map<String,Object> contentNameProperties = new HashMap<>();
        contentNameProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        contentNameProperties.put("index", "true");
        properties.put("contentid", contentNameProperties);

        Map<String,Object> contentTypeProperties = new HashMap<>();
        contentTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        contentTypeProperties.put("index", "true");
        properties.put("contentType", contentTypeProperties);

        Map<String,Object> formalProperties = new HashMap<>();
        formalProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        formalProperties.put("index", "true");
        properties.put("formal", formalProperties);

        Map<String,Object> srcTypeProperties = new HashMap<>();
        srcTypeProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        srcTypeProperties.put("index", "true");
        properties.put("srcType", srcTypeProperties);

        Map<String,Object> lastPageIdProperties = new HashMap<>();
        lastPageIdProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        lastPageIdProperties.put("index", "true");
        properties.put("srcType", lastPageIdProperties);

        Map<String,Object> stbidProperties = new HashMap<>();
        stbidProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        stbidProperties.put("index", "true");
        properties.put("stbid", stbidProperties);

        Map<String,Object> hardwareVendorInfoProperties = new HashMap<>();
        hardwareVendorInfoProperties.put("type", "keyword");
        //albumProperties.put("store", "true");
        hardwareVendorInfoProperties.put("index", "true");
        properties.put("hardwareVendorInfo", hardwareVendorInfoProperties);
*/
        Gson gson2 = new GsonBuilder().enableComplexMapKeySerialization().create();
        String json = gson2.toJson(mappings);
        System.out.println(json);

        PutMappingRequest request = Requests.putMappingRequest(INDEX).type(TYPE).source(json, XContentType.JSON);
        client.admin().indices().putMapping(request).actionGet();

    }

    public static  void bulk(String... ids) {
        IndexResponse response=null;
        try {
            TransportClient client= getClient();
            BulkRequestBuilder bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for(String id:ids){
                bulkRequest.add(client.prepareIndex("bulktest", "_doc", id)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("albumid", id)
                                .field("postDate", new Date().getTime())
                                .field("albumname", "刘家媳妇")
                                .field("parntner", "SD_CMCC_JN")
                                .field("action", 5)
                                .field("play", 100)
                                .endObject()
                        )
                );
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                System.out.println("error-------");
            }

        }catch (Exception e){

        }
    }

    public static  void bulks(String... ids) {
        IndexResponse response=null;
        try {
            TransportClient client= getClient();
            BulkRequestBuilder bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for(String id:ids){
                bulkRequest.add(client.prepareIndex("bulktest1", "_doc", id)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("albumid", id)
                                .field("channelid", id)
                                .field("channelname", "电视剧")
                                .field("sourceid", id)
                                .field("sourcename", "点播历史记录")
                                .field("time", new Date().getTime())
                                .field("albumname", "刘家媳妇")
                                .field("parntner", "SD_CMCC_JN")
                                .field("topicid", "1000000")
                                .field("topicname", "VIP")
                                .field("clientVersion", "3.0.06")
                                .field("day", "2019-02-28")
                                .field("action", 5)
                                .field("playtype", 5)
                                .field("modeltype", 5)
                                .field("smodulecompname", "测试数据")
                                .field("scontentname", "测试数据")
                                .field("scontenttype", 5)
                                .field("sposidname", "测试数据")
                                .field("sposidversion", 5)
                                .field("waterfallareatype", 5)
                                .field("swaterfallareatype", 5)
                                .field("exposedtagname", "测试数据")
                                .field("sexposedtagname", "测试数据")
                                .field("searchtagname", 5)
                                .field("searchrelationkey", 5)
                                .field("waterfallpageid", 5)
                                .field("swaterfallpageid", 5)
                                .field("waterfallpagename", "测试数据")
                                .field("swaterfallpagename", "测试数据")
                                .field("waterfallchannelid", 5)
                                .field("swaterfallchannelid", 5)
                                .field("operatetype", 5)
                                .field("actorname", 5)
                                .field("actorid", 5)
                                .field("recommendacttype", 5)
                                .field("upgrademode", 5)
                                .field("lastversion", 5)
                                .field("searchhit", 5)
                                .field("keyboardtype", 5)
                                .field("exposedtagid", 5)
                                .field("oldversion", 5)
                                .field("bandwidth", 5)
                                .field("favortype", 5)
                                .field("play", 1000000)
                                .field("online", 1000000)
                                .endObject()
                        )
                );
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                System.out.println("error-------");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static SearchResponse queryAll(){
        SearchResponse response=null;
        try {
            TransportClient client= getClient();
            response = client.prepareSearch()
                    .setFrom(0).setSize(10000).setExplain(true)
                    .get();
            // client.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }

    public static SearchResponse query(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            client.prepareGet();
            response = client.prepareSearch( "bulktest")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("albumid", value))
                    .setFrom(0).setSize(100)
                    .get();
            client.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }
    public static SearchResponse like(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            client.prepareGet();
            response = client.prepareSearch( "bulktest1")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.wildcardQuery("albumid", value))
                    .setFrom(0).setSize(100)
                    .get();
            client.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }

    public static SearchResponse count(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            ValueCountAggregationBuilder aggcount=AggregationBuilders.count("count").field("albumid");
            client.prepareSearch( "bulktest1")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(aggcount)
                    .setSize(0).setExplain(true).get();


        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }

    public static SearchResponse distinctcount(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            CardinalityAggregationBuilder aggdistinct= AggregationBuilders.cardinality("distinct_count_uid").field("albumid");
            client.prepareSearch( "bulktest1")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(aggdistinct)
                    .setSize(0).setExplain(true).get();


        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }

    public static SearchResponse groupby(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            TermsAggregationBuilder aggcgroup=AggregationBuilders.terms("group_name").field("albumid");
            client.prepareSearch( "bulktest1")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(aggcgroup)
                    .setSize(0).setExplain(true).get();


        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }

    public static SearchResponse aggTest(String value){
        SearchResponse response =null;
        try {
            TransportClient client= getClient();
            TermsAggregationBuilder agg= AggregationBuilders.terms("player_count ").field("albumid");
            client.prepareSearch( "bulktest1")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .addAggregation(agg)
                    .setSize(0).setExplain(true).get();
        }catch (Exception e){
            e.printStackTrace();
        }
        return response;
    }


}
