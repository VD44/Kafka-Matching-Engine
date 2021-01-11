import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.Serializable;
import java.util.*;

public class KProcessor {
    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exchange");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.Order().getClass());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        StoreBuilder<KeyValueStore<Long, Long>> balanceStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Balances"),
                Serdes.Long(),
                Serdes.Long());
        StoreBuilder<KeyValueStore<UUID, UUID>> positionStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Positions"),
                Serdes.UUID(),
                Serdes.UUID());
        StoreBuilder<KeyValueStore<Long, UUID>> bookStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Books"),
                Serdes.Long(),
                Serdes.UUID());
        StoreBuilder<KeyValueStore<Long, UUID>> bucketStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Buckets"),
                Serdes.Long(),
                Serdes.UUID());
        StoreBuilder<KeyValueStore<Long, Order>> orderStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Orders"),
                Serdes.Long(),
                CustomSerdes.Order());
        Topology topology = new Topology();
        topology.addSource("Source", "MatchIn")
        .addProcessor("MatchingEngine", MatchingEngine::new, "Source")
        .addStateStore(balanceStore, "MatchingEngine")
        .addStateStore(positionStore, "MatchingEngine")
        .addStateStore(bookStore, "MatchingEngine")
        .addStateStore(bucketStore, "MatchingEngine")
        .addStateStore(orderStore, "MatchingEngine")
        .addSink("Sink", "MatchOut", "MatchingEngine");
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    public static class MatchingEngine implements Processor<String, Order> {

        private final int ADD_SYMBOL = 0;
        private final int REMOVE_SYMBOL = 1;
        private final int BUY = 2;
        private final int SELL = 3;
        private final int CANCEL = 4;
        private final int BOUGHT = 5;
        private final int SOLD = 6;
        private final int REJECT = 7;
        private final int CREATE_BALANCE = 100;
        private final int TRANSFER = 101;
        private final int PAYOUT = 200;

        private ProcessorContext context;
        private KeyValueStore<Long, Long> balances;
        private KeyValueStore<UUID, UUID> positions;
        private KeyValueStore<Long, Order> orders;
        private KeyValueStore<Long, UUID> books;
        private KeyValueStore<Long, UUID> buckets;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.balances = (KeyValueStore<Long, Long>) context.getStateStore("Balances");
            this.positions = (KeyValueStore<UUID, UUID>) context.getStateStore("Positions");
            this.orders = (KeyValueStore<Long, Order>) context.getStateStore("Orders");
            this.books = (KeyValueStore<Long, UUID>) context.getStateStore("Books");
            this.buckets = (KeyValueStore<Long, UUID>) context.getStateStore("Buckets");
        }

        @Override
        public void process(String _ignore, Order order) {
            this.context.forward("IN", order);
            boolean result = false;
            switch ( order.action ) {
                case ADD_SYMBOL:
                    result = addSymbol(order.sid);
                    break;
                case REMOVE_SYMBOL:
                    result = removeSymbol(order.sid);
                    break;
                case BUY:
                case SELL:
                    result = addOrder(order);
                    break;
                case CANCEL:
                    result = removeOrder(order.oid, order.aid);
                    break;
                case PAYOUT:
                    payout(order);
                    break;
                case CREATE_BALANCE:
                    result = createBalance(order);
                    break;
                case TRANSFER:
                    result = transfer(order);
                    break;
            }
            if(!result) order.action = REJECT;
            this.context.forward("OUT", order);
            this.context.commit();
        }

        @Override
        public void close() { }

        private boolean createBalance(Order order) {
            long aid = order.aid;
            if(this.balances.get(aid) == null) {
                this.balances.put(aid, 0L);
                return true;
            }
            return false;
        }

        private boolean transfer(Order order) {
            long aid = order.aid;
            Long balance = this.balances.get(aid);
            if(balance == null || balance < -order.size) return false;
            this.balances.put(aid, balance + order.size);
            return true;
        }

        private boolean payout(Order order) {
            if(!removeSymbol(order.sid)) return false;
            List<UUID> positionsToRemove = new LinkedList<>();
            KeyValueIterator<UUID, UUID> iter = this.positions.all();
            while (iter.hasNext()) {
                KeyValue<UUID, UUID> entry = iter.next();
                UUID positionKey = entry.key;
                if(getPositionKeySid(positionKey) == order.sid) {
                    long aid = getPositionKeyAid(positionKey);
                    this.balances.put(aid, this.balances.get(aid) +
                            getPositionAmount(this.positions.get(positionKey)) * order.size);
                    positionsToRemove.add(positionKey);
                }
            }
            iter.close();
            for (UUID position : positionsToRemove) this.positions.delete(position);
            return true;
        }

        private boolean checkBalance(Order order) {
            long aid = order.aid;
            Long balance = this.balances.get(aid);
            if(balance == null) return false;
            boolean isBuy = order.action == BUY;
            int size = order.size * ( isBuy ? 1 : -1);
            UUID position = getPosition(order.aid, order.sid);
            long available = position != null ? getPositionAvailable(position) : 0;
            long adj = isBuy ? Math.max(Math.min(available,0),-size) : Math.min(Math.max(available,0),-size);
            long riskAmount = (size + adj) * (isBuy ? order.price : order.price - 100);
            if(balance < riskAmount) return false;
            this.balances.put(aid, balance - riskAmount);
            if(adj != 0) setPosition(aid, order.sid,
                    getPositionAmount(position), available - adj);
            return true;
        }

        private boolean addSymbol(long sid) {
            if(this.books.get(sid) == null) {
                this.books.put(sid, new UUID(0, 0));
                this.books.put(-sid, new UUID(0, 0));
                return true;
            }
            return false;
        }

        private boolean removeSymbol(long sid) {
            if(removeAllOrders(sid) || removeAllOrders(-sid)) return false;
            this.books.delete(sid);
            this.books.delete(-sid);
            return true;
        }

        private boolean addOrder(Order order) {
            long sid = order.sid * ( order.action == BUY ? 1 : -1);
            UUID book = this.books.get(sid);
            if(book == null || !checkBalance(order)) return false;
            if(tryMatch(order)) return true;
            book = this.books.get(sid);
            long oid = order.oid;
            int price = order.price;
            long bucketPointer = getBucketPointer(sid, price);
            if(!checkBit(book, price)) {
                this.buckets.put(bucketPointer, new UUID(oid, oid));
                this.books.put(sid, getWithBitSet(book, price));
            } else {
                UUID bucket = this.buckets.get(bucketPointer);
                long lastPointer = getLastPointer(bucket);
                Order currLast = this.orders.get(lastPointer);
                currLast.next = oid;
                order.prev = currLast.oid;
                this.orders.put(lastPointer, currLast);
                this.buckets.put(bucketPointer, new UUID(getFirstPointer(bucket), oid));
            }
            this.orders.put(oid, order);
            return true;
        }

        private boolean tryMatch(Order takerOrder) {
            boolean takerIsBuy = takerOrder.action == BUY;
            long sid = takerOrder.sid * ( takerIsBuy ? 1 : -1 );
            int price = takerOrder.price;
            UUID makerOrderBitMap = this.books.get(-sid);
            int priceBit = takerIsBuy ? getMinPriceBucketPointer(makerOrderBitMap) :
                    getMaxPriceBucketPointer(makerOrderBitMap);
            if(priceBit == -1) return false;
            long bucketPointer = getBucketPointer(-sid, priceBit);
            UUID bucket = this.buckets.get(bucketPointer);
            Long makerPointer = getFirstPointer(bucket);
            Order makerOrder = this.orders.get(makerPointer);
            while (takerOrder.size > 0 && takerIsBuy ? makerOrder.price <= price : makerOrder.price >= price){
                int tradeSize = Math.min(takerOrder.size, makerOrder.size);
                makerOrder.size -= tradeSize;
                takerOrder.size -= tradeSize;
                executeTrade(takerOrder,makerOrder,tradeSize,takerIsBuy);
                if (makerOrder.size != 0) break;
                this.orders.delete(makerOrder.oid);
                if(makerOrder.next == null) {
                    this.buckets.delete(bucketPointer);
                    makerOrderBitMap = getWithBitUnset(makerOrderBitMap, makerOrder.price);
                    this.books.put(-sid, makerOrderBitMap);
                    priceBit = takerIsBuy ? getMinPriceBucketPointer(makerOrderBitMap) :
                            getMaxPriceBucketPointer(makerOrderBitMap);
                    if(priceBit == -1) return takerOrder.size == 0;
                    bucketPointer = getBucketPointer(-sid, priceBit);
                    bucket = this.buckets.get(bucketPointer);
                    makerPointer = getFirstPointer(bucket);
                } else {
                    makerPointer = makerOrder.next;
                }
                makerOrder = this.orders.get(makerPointer);
            }
            this.buckets.put(bucketPointer, new UUID(makerPointer, getLastPointer(bucket)));
            makerOrder.prev = null;
            this.orders.put(makerPointer, makerOrder);
            return takerOrder.size == 0;
        }

        private void executeTrade(Order takerOrder, Order makerOrder, int tradeSize, boolean takerIsBuy){
            Order newMakerOrder = new Order(takerIsBuy ? SOLD : BOUGHT,
                    makerOrder.oid, makerOrder.aid, makerOrder.sid, 0, tradeSize);
            Order newTakerOrder = new Order(takerIsBuy ? BOUGHT : SOLD,
                    takerOrder.oid, takerOrder.aid, takerOrder.sid, takerOrder.price - makerOrder.price, tradeSize);
            fillOrder(newMakerOrder);
            fillOrder(newTakerOrder);
            this.context.forward("OUT", newMakerOrder);
            this.context.forward("OUT", newTakerOrder);
        }

        private void fillOrder(Order order) {
            int size = order.size * (order.action == BOUGHT ? 1 : -1);
            UUID position = getPosition(order.aid, order.sid);
            if(position == null) {
                setPosition(order.aid, order.sid, size, size);
            } else {
                long newPosition = getPositionAmount(position) + size;
                if(newPosition == 0) this.positions.delete(position);
                else setPosition(position, newPosition, getPositionAvailable(position) + size);
            }
            this.balances.put(order.aid, this.balances.get(order.aid) + size * order.price);
        }

        private boolean removeOrder(long oid, long aid) {
            Order order = this.orders.get(oid);
            if(order == null || order.aid != aid) return false;
            long sid = order.sid * ( order.action == BUY ? 1 : -1);
            int price = order.price;
            UUID book = this.books.get(sid);
            long bucketPointer = getBucketPointer(sid, price);
            UUID bucket = this.buckets.get(bucketPointer);
            Long prevPointer = order.prev;
            Long nextPointer = order.next;
            if(prevPointer == null && nextPointer == null) {
                this.buckets.delete(bucketPointer);
                this.books.put(sid, getWithBitUnset(book, price));
            } else if(prevPointer == null) {
                this.buckets.put(bucketPointer, new UUID(nextPointer, getLastPointer(bucket)));
                Order nextNode = this.orders.get(nextPointer);
                nextNode.prev = null;
                this.orders.put(nextPointer, nextNode);
            } else if(nextPointer == null) {
                this.buckets.put(bucketPointer, new UUID(getFirstPointer(bucket), prevPointer));
                Order prevNode = this.orders.get(prevPointer);
                prevNode.next = null;
                this.orders.put(prevPointer, prevNode);
            } else {
                Order prevNode = this.orders.get(prevPointer);
                Order nextNode = this.orders.get(nextPointer);
                prevNode.next = nextPointer;
                nextNode.prev = prevPointer;
                this.orders.put(prevPointer, prevNode);
                this.orders.put(nextPointer, nextNode);
            }
            this.orders.delete(oid);
            postRemoveAdjustments(order);
            return true;
        }

        private void postRemoveAdjustments(Order order) {
            boolean isBuy = order.action == BUY;
            int size = order.size * (isBuy ? 1 : -1);
            UUID position = getPosition(order.aid, order.sid);
            long positionBlocked = position != null ? getPositionAmount(position) - getPositionAvailable(position) : 0;
            long adj = isBuy ? Math.max(Math.min(positionBlocked,0),-size) : Math.min(Math.max(positionBlocked,0),-size);
            this.balances.put(order.aid, this.balances.get(order.aid) + (size + adj) * (isBuy ? order.price : order.price - 100));
            if(adj != 0) setPosition(position, getPositionAmount(position), getPositionAvailable(position) + adj);
        }

        private boolean removeAllOrders(long sid){
            List<Long> bucketPointersToRemove = new LinkedList<>();
            List<Long> orderPointersToRemove = new LinkedList<>();
            UUID book = this.books.get(sid);
            if(book == null) return false;
            int price = getMinPriceBucketPointer(book);
            while (price != -1) {
                long bucketPointer = getBucketPointer(sid, price);
                bucketPointersToRemove.add(bucketPointer);
                book = getWithBitSet(book, price);
                price = getMinPriceBucketPointer(book);
                Long orderPointer = getFirstPointer(this.buckets.get(bucketPointer));
                while(orderPointer != null){
                    Order order = this.orders.get(orderPointer);
                    postRemoveAdjustments(order);
                    orderPointersToRemove.add(orderPointer);
                    orderPointer = order.next;
                }
            }
            for (Long pointer : bucketPointersToRemove) this.buckets.delete(pointer);
            for (Long pointer : orderPointersToRemove) this.orders.delete(pointer);
            return true;
        }

        private int getMinPriceBucketPointer(UUID book) {
            if(book.getLeastSignificantBits() == 0 && book.getMostSignificantBits() == 0) return -1;
            else if(book.getLeastSignificantBits() == 0) return getFirstSetBitPos(book.getMostSignificantBits())+63;
            else return getFirstSetBitPos(book.getLeastSignificantBits());
        }

        private int getMaxPriceBucketPointer(UUID book) {
            if(book.getMostSignificantBits() == 0 && book.getLeastSignificantBits() == 0) return -1;
            else if(book.getMostSignificantBits() == 0) return getLastSetBitPos(book.getLeastSignificantBits());
            else return getLastSetBitPos(book.getMostSignificantBits())+63;
        }

        private int getFirstSetBitPos(long n) {
            return (int)((Math.log10(n & -n)) / Math.log10(2));
        }

        private int getLastSetBitPos(long n) {
            return (int)((Math.log10(n)) / Math.log10(2));
        }

        private long getBucketPointer(long sid, int price) {
            return (sid << 8) | price;
        }

        private long getFirstPointer(UUID bucket) {
            return bucket.getMostSignificantBits();
        }

        private long getLastPointer(UUID bucket) {
            return bucket.getLeastSignificantBits();
        }

        private boolean checkBit(UUID book, int price) {
            if(price < 63) return getBit(book.getLeastSignificantBits(), price);
            else return getBit(book.getMostSignificantBits(), price - 63);
        }

        private UUID getWithBitSet(UUID book, int price) {
            if(price < 63) return new UUID(book.getMostSignificantBits(),setBit(book.getLeastSignificantBits(), price));
            else return new UUID(setBit(book.getMostSignificantBits(), price - 63), book.getLeastSignificantBits());
        }

        private UUID getWithBitUnset(UUID book, int price) {
            if(price < 63) return new UUID(book.getMostSignificantBits(), unsetBit(book.getLeastSignificantBits(), price));
            else return new UUID(unsetBit(book.getMostSignificantBits(), price - 63), book.getLeastSignificantBits());
        }

        private boolean getBit(long n, int k) {
            return 1L == ((n >> k) & 1L);
        }

        private long setBit(long n, int k) {
            return n | (1L << k);
        }

        private long unsetBit(long n, int k) {
            return n & ~(1L << k);
        }

        private long getPositionAmount(UUID position){
            return position.getMostSignificantBits();
        }

        private long getPositionAvailable(UUID position){
            return position.getLeastSignificantBits();
        }

        private UUID getPosition(long aid, long sid){
            return this.positions.get(new UUID(aid, sid));
        }

        private void setPosition(long aid, long sid, long amount, long available){
            this.positions.put(new UUID(aid, sid), new UUID(amount, available));
        }

        private void setPosition(UUID position, long amount, long available){
            this.positions.put(position, new UUID(amount, available));
        }

        private long getPositionKeyAid(UUID positionKey){
            return positionKey.getMostSignificantBits();
        }

        private long getPositionKeySid(UUID positionKey){
            return positionKey.getLeastSignificantBits();
        }
    }
}

@JsonRootName("order")
class Order implements Serializable {

    public int action;
    public long oid;
    public long aid;
    public long sid;
    public int price;
    public int size;
    public Long next;
    public Long prev;

    public Order() { }

    @JsonCreator
    public Order(@JsonProperty("action") int action,
          @JsonProperty("oid") long oid, @JsonProperty("aid") long aid, @JsonProperty("sid") long sid,
          @JsonProperty("price") int price, @JsonProperty("size") int size){
        this.action = action;
        this.oid = oid;
        this.aid = aid;
        this.sid = sid;
        this.price = price;
        this.size = size;
        this.next = null;
        this.prev = null;
    }
}

class JsonSerializer<T> implements Serializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void close() { }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            throw new SerializationException();
        }
    }
}

class JsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> type;

    public JsonDeserializer() { }

    JsonDeserializer(Class<T> type) { this.type = type; }

    @Override
    public void close() { }

    @Override
    public void configure(Map<String, ?> map, boolean arg1) { }

    @Override
    public T deserialize(String undefined, byte[] bytes) {
        if (bytes == null) return null;
        try {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}

final class CustomSerdes {
    static public final class OrderSerde extends Serdes.WrapperSerde<Order> {
        public OrderSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Order.class));
        }
    }
    public static Serde<Order> Order() { return new OrderSerde(); }
}