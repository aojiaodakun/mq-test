//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.bes.mq.command;

import com.bes.mq.BESMQConnection;
import com.bes.mq.BESMQMessageProducer;
import com.bes.mq.broker.region.Destination;
import com.bes.mq.broker.region.MessageReference;
import com.bes.mq.protocolformat.ProtocolFormat;
import com.bes.mq.usage.MemoryUsage;
import com.bes.mq.util.ByteArrayInputStream;
import com.bes.mq.util.ByteArrayOutputStream;
import com.bes.mq.util.ByteSequence;
import com.bes.mq.util.MarshallingSupport;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import javax.jms.JMSException;

public abstract class Message extends BaseCommand implements MarshallAware, MessageReference, Serializable {
    public static final int DEFAULT_MINIMUM_MESSAGE_SIZE = 1024;
    protected MessageId messageId;
    protected BESMQDestination originalDestination;
    protected TransactionId originalTransactionId;
    protected ProducerId producerId;
    protected BESMQDestination destination;
    protected TransactionId transactionId;
    protected long expiration;
    protected long timestamp;
    protected long arrival;
    protected long brokerInTime;
    protected long brokerOutTime;
    protected String correlationId;
    protected BESMQDestination replyTo;
    protected boolean persistent;
    protected String type;
    protected byte priority;
    protected String groupID;
    protected int groupSequence;
    protected ConsumerId targetConsumerId;
    protected boolean compressed;
    protected String userID;
    protected ByteSequence content;
    protected ByteSequence marshalledProperties;
    protected DataStructure dataStructure;
    protected int redeliveryCounter;
    protected int size;
    protected Map<String, Object> properties;
    protected boolean readOnlyProperties;
    protected boolean readOnlyBody;
    protected transient boolean recievedByDFBridge;
    protected boolean droppable;
    private transient short referenceCount;
    private transient BESMQMessageProducer messageProducer;
    private transient BESMQConnection connection;
    private transient Destination regionDestination;
    private transient MemoryUsage memoryUsage;
    private BrokerId[] brokerPath;
    private BrokerId[] cluster;

    public Message() {
    }

    public abstract Message copy();

    public abstract void clearBody() throws JMSException;

    public abstract void storeContent();

    public void clearMarshalledState() throws JMSException {
        this.properties = null;
    }

    protected void copy(Message copy) {
        super.copy(copy);
        copy.producerId = this.producerId;
        copy.transactionId = this.transactionId;
        copy.destination = this.destination;
        copy.messageId = this.messageId != null ? this.messageId.copy() : null;
        copy.originalDestination = this.originalDestination;
        copy.originalTransactionId = this.originalTransactionId;
        copy.expiration = this.expiration;
        copy.timestamp = this.timestamp;
        copy.correlationId = this.correlationId;
        copy.replyTo = this.replyTo;
        copy.persistent = this.persistent;
        copy.redeliveryCounter = this.redeliveryCounter;
        copy.type = this.type;
        copy.priority = this.priority;
        copy.size = this.size;
        copy.groupID = this.groupID;
        copy.userID = this.userID;
        copy.groupSequence = this.groupSequence;
        if (this.properties != null) {
            copy.properties = new HashMap(this.properties);
            copy.properties.remove("originalExpiration");
        } else {
            copy.properties = this.properties;
        }

        copy.content = this.content;
        copy.marshalledProperties = this.marshalledProperties;
        copy.dataStructure = this.dataStructure;
        copy.readOnlyProperties = this.readOnlyProperties;
        copy.readOnlyBody = this.readOnlyBody;
        copy.compressed = this.compressed;
        copy.recievedByDFBridge = this.recievedByDFBridge;
        copy.arrival = this.arrival;
        copy.messageProducer = this.messageProducer;
        copy.connection = this.connection;
        copy.regionDestination = this.regionDestination;
        copy.brokerInTime = this.brokerInTime;
        copy.brokerOutTime = this.brokerOutTime;
        copy.memoryUsage = this.memoryUsage;
        copy.brokerPath = this.brokerPath;
    }

    public Object getProperty(String name) throws IOException {
        if (this.properties == null) {
            if (this.marshalledProperties == null) {
                return null;
            }

            this.properties = this.unmarsallProperties(this.marshalledProperties);
        }

        return this.properties.get(name);
    }

    public Map<String, Object> getProperties() throws IOException {
        if (this.properties == null) {
            if (this.marshalledProperties == null) {
                return Collections.EMPTY_MAP;
            }

            this.properties = this.unmarsallProperties(this.marshalledProperties);
        }

        return Collections.unmodifiableMap(this.properties);
    }

    public void clearProperties() {
        this.marshalledProperties = null;
        this.properties = null;
    }

    public void setProperty(String name, Object value) throws IOException {
        this.lazyCreateProperties();
        this.properties.put(name, value);
    }

    public void removeProperty(String name) throws IOException {
        this.lazyCreateProperties();
        this.properties.remove(name);
    }

    protected void lazyCreateProperties() throws IOException {
        if (this.properties == null) {
            if (this.marshalledProperties == null) {
                this.properties = new HashMap();
            } else {
                this.properties = this.unmarsallProperties(this.marshalledProperties);
                this.marshalledProperties = null;
            }
        }

    }

    private Map<String, Object> unmarsallProperties(ByteSequence marshalledProperties) throws IOException {
        return MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(marshalledProperties)));
    }

    public void beforeMarshall(ProtocolFormat protocolFormat) throws IOException {
        if (this.marshalledProperties == null && this.properties != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos);
            MarshallingSupport.marshalPrimitiveMap(this.properties, os);
            os.close();
            this.marshalledProperties = baos.toByteSequence();
        }

    }

    public void afterMarshall(ProtocolFormat protocolFormat) throws IOException {
    }

    public void beforeUnmarshall(ProtocolFormat protocolFormat) throws IOException {
    }

    public void afterUnmarshall(ProtocolFormat protocolFormat) throws IOException {
    }

    public ProducerId getProducerId() {
        return this.producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    public BESMQDestination getDestination() {
        return this.destination;
    }

    public void setDestination(BESMQDestination destination) {
        this.destination = destination;
    }

    public TransactionId getTransactionId() {
        return this.transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isInTransaction() {
        return this.transactionId != null;
    }

    public BESMQDestination getOriginalDestination() {
        return this.originalDestination;
    }

    public void setOriginalDestination(BESMQDestination destination) {
        this.originalDestination = destination;
    }

    public MessageId getMessageId() {
        return this.messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public TransactionId getOriginalTransactionId() {
        return this.originalTransactionId;
    }

    public void setOriginalTransactionId(TransactionId transactionId) {
        this.originalTransactionId = transactionId;
    }

    public String getGroupID() {
        return this.groupID;
    }

    public void setGroupID(String groupID) {
        this.groupID = groupID;
    }

    public int getGroupSequence() {
        return this.groupSequence;
    }

    public void setGroupSequence(int groupSequence) {
        this.groupSequence = groupSequence;
    }

    public String getCorrelationId() {
        return this.correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public boolean isPersistent() {
        return this.persistent;
    }

    public void setPersistent(boolean deliveryMode) {
        this.persistent = deliveryMode;
    }

    public long getExpiration() {
        return this.expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public byte getPriority() {
        return this.priority;
    }

    public void setPriority(byte priority) {
        if (priority < 0) {
            this.priority = 0;
        } else if (priority > 9) {
            this.priority = 9;
        } else {
            this.priority = priority;
        }

    }

    public BESMQDestination getReplyTo() {
        return this.replyTo;
    }

    public void setReplyTo(BESMQDestination replyTo) {
        this.replyTo = replyTo;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ByteSequence getContent() {
        return this.content;
    }

    public void setContent(ByteSequence content) {
        this.content = content;
    }

    public ByteSequence getMarshalledProperties() {
        return this.marshalledProperties;
    }

    public void setMarshalledProperties(ByteSequence marshalledProperties) {
        this.marshalledProperties = marshalledProperties;
    }

    public DataStructure getDataStructure() {
        return this.dataStructure;
    }

    public void setDataStructure(DataStructure data) {
        this.dataStructure = data;
    }

    public ConsumerId getTargetConsumerId() {
        return this.targetConsumerId;
    }

    public void setTargetConsumerId(ConsumerId targetConsumerId) {
        this.targetConsumerId = targetConsumerId;
    }

    public boolean isExpired() {
        long expireTime = this.getExpiration();
        return expireTime > 0L && System.currentTimeMillis() > expireTime;
    }

    public boolean isNotification() {
        return this.type != null && this.type.equals("Notification");
    }

    public boolean isCompressed() {
        return this.compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public boolean isRedelivered() {
        return this.redeliveryCounter > 0;
    }

    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!this.isRedelivered()) {
                this.setRedeliveryCounter(1);
            }
        } else if (this.isRedelivered()) {
            this.setRedeliveryCounter(0);
        }

    }

    public void incrementRedeliveryCounter() {
        ++this.redeliveryCounter;
    }

    public int getRedeliveryCounter() {
        return this.redeliveryCounter;
    }

    public void setRedeliveryCounter(int deliveryCounter) {
        this.redeliveryCounter = deliveryCounter;
    }

    public BrokerId[] getBrokerPath() {
        return this.brokerPath;
    }

    public void setBrokerPath(BrokerId[] brokerPath) {
        this.brokerPath = brokerPath;
    }

    public boolean isReadOnlyProperties() {
        return this.readOnlyProperties;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    public boolean isReadOnlyBody() {
        return this.readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public BESMQMessageProducer getMessageProducer() {
        return this.messageProducer;
    }

    public void setMessageProducer(BESMQMessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    public BESMQConnection getConnection() {
        return this.connection;
    }

    public void setConnection(BESMQConnection connection) {
        this.connection = connection;
    }

    public long getArrival() {
        return this.arrival;
    }

    public void setArrival(long arrival) {
        this.arrival = arrival;
    }

    public String getUserID() {
        return this.userID;
    }

    public void setUserID(String jmsxUserID) {
        this.userID = jmsxUserID;
    }

    public int getReferenceCount() {
        return this.referenceCount;
    }

    public Message getMessageHardRef() {
        return this;
    }

    public Message getMessage() {
        return this;
    }

    public Destination getRegionDestination() {
        return this.regionDestination;
    }

    public void setRegionDestination(Destination destination) {
        this.regionDestination = destination;
        if (this.memoryUsage == null) {
            this.memoryUsage = this.regionDestination.getMemoryUsage();
        }

    }

    public MemoryUsage getMemoryUsage() {
        return this.memoryUsage;
    }

    public void setMemoryUsage(MemoryUsage usage) {
        this.memoryUsage = usage;
    }

    public boolean isMarshallAware() {
        return true;
    }

    public int incrementReferenceCount() {
        short rc;
        int size;
        synchronized(this) {
            rc = ++this.referenceCount;
            size = this.getSize();
        }

        if (rc == 1 && this.getMemoryUsage() != null) {
            this.getMemoryUsage().increaseUsage((long)size);
        }

        return rc;
    }

    public int decrementReferenceCount() {
        short rc;
        int size;
        synchronized(this) {
            rc = --this.referenceCount;
            size = this.getSize();
        }

        if (rc == 0 && this.getMemoryUsage() != null) {
            this.getMemoryUsage().decreaseUsage((long)size);
        }

        return rc;
    }

    public int getSize() {
        int minimumMessageSize = this.getMinimumMessageSize();
        if (this.size < minimumMessageSize || this.size == 0) {
            this.size = minimumMessageSize;
            if (this.marshalledProperties != null) {
                this.size += this.marshalledProperties.getLength();
            }

            if (this.content != null) {
                this.size += this.content.getLength();
            }
        }

        return this.size;
    }

    protected int getMinimumMessageSize() {
        int result = 1024;
        Destination dest = this.regionDestination;
        if (dest != null) {
            result = dest.getMinimumMessageSize();
        }

        return result;
    }

    public boolean isRecievedByDFBridge() {
        return this.recievedByDFBridge;
    }

    public void setRecievedByDFBridge(boolean recievedByDFBridge) {
        this.recievedByDFBridge = recievedByDFBridge;
    }

    public void onMessageRolledBack() {
        this.incrementRedeliveryCounter();
    }

    public boolean isDroppable() {
        return this.droppable;
    }

    public void setDroppable(boolean droppable) {
        this.droppable = droppable;
    }

    public BrokerId[] getCluster() {
        return this.cluster;
    }

    public void setCluster(BrokerId[] cluster) {
        this.cluster = cluster;
    }

    public boolean isMessage() {
        return true;
    }

    public long getBrokerInTime() {
        return this.brokerInTime;
    }

    public void setBrokerInTime(long brokerInTime) {
        this.brokerInTime = brokerInTime;
    }

    public long getBrokerOutTime() {
        return this.brokerOutTime;
    }

    public void setBrokerOutTime(long brokerOutTime) {
        this.brokerOutTime = brokerOutTime;
    }

    public boolean isDropped() {
        return false;
    }

    public void compress() throws IOException {
        if (!this.isCompressed()) {
            this.storeContent();
            if (!this.isCompressed() && this.getContent() != null) {
                this.doCompress();
            }
        }

    }

    protected void doCompress() throws IOException {
        this.compressed = true;
        ByteSequence bytes = this.getContent();
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        OutputStream os = new DeflaterOutputStream(bytesOut);
        os.write(bytes.data, bytes.offset, bytes.length);
        os.close();
        this.setContent(bytesOut.toByteSequence());
    }

    public String toString() {
        return this.toString((Map)null);
    }

    public String toString(Map<String, Object> overrideFields) {
        try {
            this.getProperties();
        } catch (IOException var3) {
        }

        return super.toString(overrideFields);
    }
}
