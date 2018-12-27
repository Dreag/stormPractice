package edu.yang.storm.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

public class OrderDataGenerator {
    // order记录
    // "timestamp" "consumer" "productName" "price" "country" "province" "city"

    private static final String[] CONSUMERS = { "Merry", "John", "Tom", "Candy",
            "张三丰", "周芷若", "张无忌", "令狐冲", "独孤九剑","郭靖", "杨过" };

    private static final String[] PRODUCT_NAMES = { "华为笔记本", "iPad", "苹果电脑", "iPhone",
            "乐视TV", "美的空调", "小米2", "魅族" };

    private static final Map<String, Double> PRODUCT_PRICE = new HashMap<String, Double>();

    static {
        PRODUCT_PRICE.put("华为笔记本", 2345.89);
        PRODUCT_PRICE.put("iPad", 3567.78);
        PRODUCT_PRICE.put("苹果电脑", 23456.12);
        PRODUCT_PRICE.put("iPhone", 6732.81);
        PRODUCT_PRICE.put("乐视TV", 1234.76);
        PRODUCT_PRICE.put("美的空调", 1260.32);
        PRODUCT_PRICE.put("小米2", 2390.81);
        PRODUCT_PRICE.put("魅族", 3456.72);
    }

    private static final String[] ADDRESSES = { "中国,上海,浦东", "中国,上海,杨浦", "中国,福建,厦门",
            "中国,浙江,杭州", "中国,江苏,苏州", "中国,北京,通州",
            "中国,北京,海淀" };

    // "timestamp" "consumer" "productName" "price" "country" "province" "city"
    /**
     * 模拟生成订单消费记录
     *
     * @return
     */

    public static String generateOrderRecord(){
        long timestamp = System.currentTimeMillis();
        StringBuilder stringBuilder = new StringBuilder("\"" + timestamp + "\"");

        Random random = new Random();
        String consumer = CONSUMERS[random.nextInt(CONSUMERS.length)];

        stringBuilder.append("\"" + consumer + "\"");
        String productName = PRODUCT_NAMES[random.nextInt(PRODUCT_NAMES.length)];
        double price = PRODUCT_PRICE.get(productName);
        stringBuilder.append(" \"" + price + "\"");
        String address = ADDRESSES[random.nextInt(ADDRESSES.length)];
        String[] addrInfos = address.split(",");
        stringBuilder.append(" \"" + addrInfos[0]  + "\"");
        stringBuilder.append(" \"" + addrInfos[1] + "\"");
        stringBuilder.append(" \"" + addrInfos[2] + "\"");

        return stringBuilder.toString();
    }

    public static void main(String [] args){
        KafkaProducer kafkaProducer = new KafkaProducer();
        Producer<String,String> producer = kafkaProducer.getKafkaProducer("10.113.9.108:9092");

        for (; ; ) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String msgKey = System.currentTimeMillis()+ "";
            String msg = OrderDataGenerator.generateOrderRecord();
            // 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage<String, String> data = kafkaProducer.getKeyedMessage("test", msgKey, msg);

            kafkaProducer.sendMassage(producer, data);
        }
        //kafkaProducer.close(producer);
    }
}
