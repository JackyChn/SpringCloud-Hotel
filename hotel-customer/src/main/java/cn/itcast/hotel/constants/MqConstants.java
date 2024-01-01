package cn.itcast.hotel.constants;

public class MqConstants {
//    exchange name
    public final static String HOTEL_EXCHANGE = "hotel.topic";
//    insert topic queue
    public final static String HOTEL_INSERT_QUEUE = "hotel.insert.queue";
//    insert routing key
    public final static String HOTEL_INSERT_KEY = "hotel.insert";
//    delete topic queue
    public final static String HOTEL_DELETE_QUEUE = "hotel.delete.queue";
//    delete routing key
    public final static String HOTEL_DELETE_KEY = "hotel.delete";

}
