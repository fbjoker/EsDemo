package cn.gitv.es.test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataMaping {
    public static Map<String,String> maping=new HashMap<String,String>();
    public static Map<String,String> maping10=new HashMap<String,String>();
    public static Map<String,String> maping20=new HashMap<String,String>();
    public static Map<String,String> maping50=new HashMap<String,String>();

    public static void maping(){
        maping.put("logDate","string");
        maping.put("day","string");
        maping.put("action","int");
        maping.put("logId","string");
        maping.put("productId","int");
        maping.put("ip","string");
        maping.put("pingbackVersion","string");
        maping.put("clientVersion","string");
        maping.put("appMark","string");
        maping.put("clientTime","string");
        maping.put("albumId","Long");
        maping.put("videoId","String");
        maping.put("dataRateId","String");
        maping.put("videoDuration","Int");
        maping.put("channelId","Int");
        maping.put("actionSrc","Int");
        maping.put("actionSubSrcName","String");
        maping.put("contentSrcId","Int");
        maping.put("videoSrcId","Int");
        maping.put("videoType","Int");
        maping.put("topicId","string");
        maping.put("topicName","String");
        maping.put("position","Int");
        maping.put("cdn","String");
        maping.put("playOffset","Int");
        maping.put("x","Int");
        maping.put("y","Int");
        maping.put("videoSrcCdn","String");
        maping.put("userAgent","String");
        maping.put("playDuration","Int");
        maping.put("clientPlayDuration","Int");
        maping.put("userId","String");
        maping.put("resolution","String");
        maping.put("videoName","String");
        maping.put("continuePlay","Int");
        maping.put("processStartTime","String");
        maping.put("processEndTime","String");
        maping.put("osVersion","String");
        maping.put("srankingId","int");
        maping.put("srankingName","string");
        maping.put("playType","Int");
        maping.put("bufferType","Int");
        maping.put("pauseDuration","Int");
        maping.put("bufferDuration","Int");
        maping.put("ls","Long");
        maping.put("repair","Int");
        maping.put("serialNumber","string");
        maping.put("searchKey","string");
        maping.put("searchType","string");
        maping.put("tagId","string");
        maping.put("tagName","string");
        maping.put("orderOptType","int");
        maping.put("prodPackageId","String");
        maping.put("payType","Int");
        maping.put("payAccount","string");
        maping.put("result","Int");
        maping.put("payPosition","string");
        maping.put("videoPrice","double");
        maping.put("actorName","string");
        maping.put("actorId","string");
        maping.put("recommendActType","string");
        maping.put("upgradeMode","Int");
        maping.put("lastVersion","String");
        maping.put("searchHit","Int");
        maping.put("wallpaper","String");
        maping.put("keyboardType","Int");
        maping.put("modelId","String");
        maping.put("modelName","String");
        maping.put("exposedTagId","String");
        maping.put("oldVersion","String");
        maping.put("bandwidth","String");
        maping.put("favorType","Int");
        maping.put("rankingId","Int");
        maping.put("rankingName","String");
        maping.put("voiceCommand","String");
        maping.put("searchMode","Int");
        maping.put("favorMode","Int");
        maping.put("h5v","String");
        maping.put("pageId","String");
        maping.put("pageName","String");
        maping.put("pageType","String");
        maping.put("entryType","Int");
        maping.put("termType","Int");
        maping.put("h5OptId","String");
        maping.put("h5OptName","String");
        maping.put("qua","Int");
        maping.put("win","Int");
        maping.put("contentid","String");
        maping.put("contentName","String");
        maping.put("contentType","String");
        maping.put("formal","Int");
        maping.put("srcType","Int");
        maping.put("lastPageId","String");
        maping.put("stbid","String");
        maping.put("hardwareVendorInfo","String");
        maping.put("areaId","String");
        maping.put("areaOrder","Int");
        maping.put("clientServiceDuration","Int");
        maping.put("sspecExtId","String");
        maping.put("stopicName","String");
        maping.put("sexposedTagId","String");
        maping.put("adsid","String");
        maping.put("adsn","String");
        maping.put("adss","Int");
        maping.put("adid","String");
        maping.put("adn","String");
        maping.put("adt","Int");
        maping.put("ado","Int");
        maping.put("adsi","Int");
        maping.put("sareaName","String");
        maping.put("sareaId","String");
        maping.put("sareaOrder","String");
        maping.put("waterfallAreaType","Int");
        maping.put("swaterfallAreaType","Int");
        maping.put("exposedTagName","String");
        maping.put("sexposedTagName","String");
        maping.put("searchTagId","String");
        maping.put("searchTagName","String");
        maping.put("searchRelationKey","String");
        maping.put("waterfallPageId","String");
        maping.put("swaterfallPageId","String");
        maping.put("waterfallPageName","String");
        maping.put("swaterfallPageName","String");
        maping.put("waterfallChannelId","String");
        maping.put("swaterfallChannelId","String");
        maping.put("operateType","Int");
        maping.put("kwpos","Int");
        maping.put("productSuitesName","String");
        maping.put("effectiveTime","String");
        maping.put("unsubscribeType","Int");
        maping.put("listType","Int");
        maping.put("sAlbumId","Long");
        maping.put("sAlbumName","String");
        maping.put("sChannelId","Int");
        maping.put("sCannelName","String");
        maping.put("swTagId","String");
        maping.put("swTagName","String");
        maping.put("pTrafficSources","Int");
        maping.put("playRecordExp","String");
        maping.put("loop","Int");
        maping.put("sModuleCompId","String");
        maping.put("sModuleCompName","String");
        maping.put("sContentid","String");
        maping.put("sContentName","String");
        maping.put("sContentType","Int");
        maping.put("prodPackageType","Int");
        maping.put("modelType","String");
        maping.put("trigMode","Int");
        maping.put("guideOperateContent","Int");
        maping.put("babyNickName","String");
        maping.put("babysex","String");
        maping.put("babyBirthdate","String");
        maping.put("durationOnoffState","Int");
        maping.put("timePlayDuration","String");
        maping.put("dayPlayDuration","String");
        maping.put("moduleCompId","String");
        maping.put("moduleCompName","String");
        maping.put("textTheme","string");
        maping.put("gotoTargetType","int");
        maping.put("parser_time","string");
        maping.put("ad_id_k","String");
        maping.put("ad_id_p","String");
        maping.put("ad_id_a","String");
        maping.put("ad_id_b","String");
        maping.put("ad_id_c","String");
        maping.put("partialtype","string");
    }

    public static void maping10() {
        maping10.put("day", "string");
        maping10.put("action", "int");
        maping10.put("logId", "string");
        maping10.put("productId", "int");
        maping10.put("ip", "string");
        maping10.put("pingbackVersion", "string");
        maping10.put("clientVersion", "string");
        maping10.put("appMark", "string");
        maping10.put("clientTime", "string");
    }

    public static void maping20() {
        maping20.put("logDate", "string");
        maping20.put("day", "string");
        maping20.put("action", "int");
        maping20.put("logId", "string");
        maping20.put("productId", "int");
        maping20.put("ip", "string");
        maping20.put("pingbackVersion", "string");
        maping20.put("clientVersion", "string");
        maping20.put("appMark", "string");
        maping20.put("clientTime", "string");
        maping20.put("videoId","String");
        maping20.put("dataRateId","String");
        maping20.put("videoDuration","Int");
        maping20.put("channelId","Int");
        maping20.put("actionSrc","Int");
        maping20.put("actionSubSrcName","String");
        maping20.put("contentSrcId","Int");
        maping20.put("videoSrcId","Int");
        maping20.put("videoType","Int");
        maping20.put("topicId","string");
    }


    public static void maping50(){
        maping50.put("logDate","string");
        maping50.put("day","string");
        maping50.put("action","int");
        maping50.put("logId","string");
        maping50.put("productId","int");
        maping50.put("ip","string");
        maping50.put("pingbackVersion","string");
        maping50.put("clientVersion","string");
        maping50.put("appMark","string");
        maping50.put("clientTime","string");
        maping50.put("albumId","Long");
        maping50.put("videoId","String");
        maping50.put("dataRateId","String");
        maping50.put("videoDuration","Int");
        maping50.put("channelId","Int");
        maping50.put("actionSrc","Int");
        maping50.put("actionSubSrcName","String");
        maping50.put("contentSrcId","Int");
        maping50.put("videoSrcId","Int");
        maping50.put("videoType","Int");
        maping50.put("topicId","string");
        maping50.put("topicName","String");
        maping50.put("position","Int");
        maping50.put("cdn","String");
        maping50.put("playOffset","Int");
        maping50.put("x","Int");
        maping50.put("y","Int");
        maping50.put("videoSrcCdn","String");
        maping50.put("userAgent","String");
        maping50.put("playDuration","Int");
        maping50.put("clientPlayDuration","Int");
        maping50.put("userId","String");
        maping50.put("resolution","String");
        maping50.put("videoName","String");
        maping50.put("continuePlay","Int");
        maping50.put("processStartTime","String");
        maping50.put("processEndTime","String");
        maping50.put("osVersion","String");
        maping50.put("srankingId","int");
        maping50.put("srankingName","string");
        maping50.put("playType","Int");
        maping50.put("bufferType","Int");
        maping50.put("pauseDuration","Int");
        maping50.put("bufferDuration","Int");
        maping50.put("ls","Long");
        maping50.put("repair","Int");
        maping50.put("serialNumber","string");
        maping50.put("searchKey","string");
        maping50.put("searchType","string");
        maping50.put("tagId","string");
    }

    public static synchronized ConcurrentHashMap<String,String> getRandomValue(int num){
        ConcurrentHashMap<String,String> fields=new ConcurrentHashMap<String,String>();
        try {
            Set<String> keys=new HashSet<String>();
            maping10();
            maping20();
            maping50();
            maping();
            if(num==10){
                keys= maping10.keySet();
            }
            if(num==20){
                keys= maping20.keySet();
            }

            if(num==50){
                keys= maping50.keySet();
            }

            if(num==100){
                keys= maping.keySet();
            }
            List<String> list=new ArrayList<String>(keys);
            if(list.size()>0){
                int rang=0;
                if (num>list.size()){
                    rang=list.size();
                }else{
                    rang=num;
                }
                List<String> sublist=list.subList(0,rang);
                // System.out.println("sublist="+sublist.toString());
                for(String key:sublist){
                    if(num==10){
                        fields.put(key,maping10.get(key));
                    }
                    if(num==20){
                        fields.put(key,maping20.get(key));
                    }
                    if(num==50){
                        fields.put(key,maping50.get(key));
                    }
                    if(num==100){
                        fields.put(key,maping.get(key));
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
       //System.out.println(fields);
        return fields;
    }

    public static void main(String [] args) {
        getRandomValue(10);
    }
}
