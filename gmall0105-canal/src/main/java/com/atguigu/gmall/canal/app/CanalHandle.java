package com.atguigu.gmall.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall0105.realtime.util.RedisUtil;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author Huangfeng
 * @create 2019-06-28 下午 6:47
 */
public class CanalHandle {

    private List<CanalEntry.RowData> rowDatasList;
    String tableName;
    CanalEntry.EventType eventType;

    public CanalHandle(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_info")) {
            sendRowListToKafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ((eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE)) && tableName.equals("user_info")) {
            sendRowListToKafka(GmallConstants.KAFKA_TOPIC_USER);
        } else if(eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_detail")){
            sendRowListToKafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }
    }

    private void sendRowListToKafka(String kafkaTopic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "->" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            //保存有变化的用户信息到redis中
            if (GmallConstants.KAFKA_TOPIC_USER.equals(kafkaTopic)) {
                Jedis jedisClient = RedisUtil.getJedisClient();
                jedisClient.hset("user_info", jsonObject.get("id").toString(), jsonObject.toJSONString());
                jedisClient.close();
            }

            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }
}
