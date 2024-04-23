//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.bes.mq.command;

import com.bes.mq.BESMQConnection;
import com.bes.mq.ScheduledMessage;
import com.bes.mq.broker.scheduler.CronParser;
import com.bes.mq.filter.PropertyExpression;
import com.bes.mq.protocolformat.ProtocolFormat;
import com.bes.mq.state.CommandVisitor;
import com.bes.mq.util.ByteSequence;
import com.bes.mq.util.Callback;
import com.bes.mq.util.JMSExceptionSupport;
import com.bes.mq.util.TypeConversionSupport;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

public class BESMQMessage extends Message implements com.bes.mq.Message, ScheduledMessage, Serializable {
    public static final byte DATA_STRUCTURE_TYPE = 34;
    public static final String DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";
    private static final Map<String, BESMQMessage.PropertySetter> JMS_PROPERTY_SETERS = new HashMap();
    protected transient Callback acknowledgeCallback;

    public BESMQMessage() {
    }

    public byte getDataStructureType() {
        return 34;
    }

    public Message copy() {
        BESMQMessage copy = new BESMQMessage();
        this.copy(copy);
        return copy;
    }

    protected void copy(BESMQMessage copy) {
        super.copy(copy);
        copy.acknowledgeCallback = this.acknowledgeCallback;
    }

    public int hashCode() {
        MessageId id = this.getMessageId();
        return id != null ? id.hashCode() : super.hashCode();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == this.getClass()) {
            BESMQMessage msg = (BESMQMessage)o;
            MessageId oMsg = msg.getMessageId();
            MessageId thisMsg = this.getMessageId();
            return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
        } else {
            return false;
        }
    }

    public void acknowledge() throws JMSException {
        if (this.acknowledgeCallback != null) {
            try {
                this.acknowledgeCallback.execute();
            } catch (JMSException var2) {
                throw var2;
            } catch (Throwable var3) {
                throw JMSExceptionSupport.create(var3);
            }
        }

    }

    public void clearBody() throws JMSException {
        this.setContent((ByteSequence)null);
        this.readOnlyBody = false;
    }

    public String getJMSMessageID() {
        MessageId messageId = this.getMessageId();
        return messageId == null ? null : messageId.toString();
    }

    public void setJMSMessageID(String value) throws JMSException {
        if (value != null) {
            try {
                MessageId id = new MessageId(value);
                this.setMessageId(id);
            } catch (NumberFormatException var4) {
                MessageId id = new MessageId();
                id.setTextView(value);
                this.setMessageId(this.messageId);
            }
        } else {
            this.setMessageId((MessageId)null);
        }

    }

    public void setJMSMessageID(ProducerId producerId, long producerSequenceId) throws JMSException {
        MessageId id = null;

        try {
            id = new MessageId(producerId, producerSequenceId);
            this.setMessageId(id);
        } catch (Throwable var6) {
            throw JMSExceptionSupport.create("Invalid message id '" + id + "', reason: " + var6.getMessage(), var6);
        }
    }

    public long getJMSTimestamp() {
        return this.getTimestamp();
    }

    public void setJMSTimestamp(long timestamp) {
        this.setTimestamp(timestamp);
    }

    public String getJMSCorrelationID() {
        return this.getCorrelationId();
    }

    public void setJMSCorrelationID(String correlationId) {
        this.setCorrelationId(correlationId);
    }

    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return encodeString(this.getCorrelationId());
    }

    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        this.setCorrelationId(decodeString(correlationId));
    }

    protected boolean isEncrypted() throws IOException {
        Boolean encrypted = (Boolean)this.getProperty("BESMQ_ENCRYPTED");
        return encrypted != null && encrypted;
    }

    protected void setEncrypted(boolean encrypted) throws IOException {
        if (encrypted) {
            this.setProperty("BESMQ_ENCRYPTED", Boolean.TRUE);
        } else {
            this.removeProperty("BESMQ_ENCRYPTED");
        }

    }

    public String getJMSXMimeType() {
        return "jms/message";
    }

    protected static String decodeString(byte[] data) throws JMSException {
        try {
            return data == null ? null : new String(data, "UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new JMSException("Invalid UTF-8 encoding: " + var2.getMessage());
        }
    }

    protected static byte[] encodeString(String data) throws JMSException {
        try {
            return data == null ? null : data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new JMSException("Invalid UTF-8 encoding: " + var2.getMessage());
        }
    }

    public Destination getJMSReplyTo() {
        return this.getReplyTo();
    }

    public void setJMSReplyTo(Destination destination) throws JMSException {
        this.setReplyTo(BESMQDestination.transform(destination));
    }

    public Destination getJMSDestination() {
        return this.getDestination();
    }

    public void setJMSDestination(Destination destination) throws JMSException {
        this.setDestination(BESMQDestination.transform(destination));
    }

    public int getJMSDeliveryMode() {
        return this.isPersistent() ? 2 : 1;
    }

    public void setJMSDeliveryMode(int mode) {
        this.setPersistent(mode == 2);
    }

    public boolean getJMSRedelivered() {
        return this.isRedelivered();
    }

    public void setJMSRedelivered(boolean redelivered) {
        this.setRedelivered(redelivered);
    }

    public String getJMSType() {
        return this.getType();
    }

    public void setJMSType(String type) {
        this.setType(type);
    }

    public long getJMSExpiration() {
        return this.getExpiration();
    }

    public void setJMSExpiration(long expiration) {
        this.setExpiration(expiration);
    }

    public int getJMSPriority() {
        return this.getPriority();
    }

    public void setJMSPriority(int priority) {
        this.setPriority((byte)priority);
    }

    public void clearProperties() {
        super.clearProperties();
        this.readOnlyProperties = false;
    }

    public boolean propertyExists(String name) throws JMSException {
        try {
            return this.getProperties().containsKey(name) || this.getObjectProperty(name) != null;
        } catch (IOException var3) {
            throw JMSExceptionSupport.create(var3);
        }
    }

    public Enumeration getPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector(this.getProperties().keySet());
            if (!result.contains("JMSXDeliveryCount")) {
                result.add("JMSXDeliveryCount");
            }

            return result.elements();
        } catch (IOException var2) {
            throw JMSExceptionSupport.create(var2);
        }
    }

    public Enumeration getAllPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector(this.getProperties().keySet());
            result.addAll(JMS_PROPERTY_SETERS.keySet());
            return result.elements();
        } catch (IOException var2) {
            throw JMSExceptionSupport.create(var2);
        }
    }

    public void setObjectProperty(String name, Object value) throws JMSException {
        this.setObjectProperty(name, value, true);
    }

    public void setObjectProperty(String name, Object value, boolean checkReadOnly) throws JMSException {
        if (checkReadOnly) {
            this.checkReadOnlyProperties();
        }

        if (name != null && !name.equals("")) {
            this.checkValidObject(value);
            value = this.convertScheduled(name, value);
            BESMQMessage.PropertySetter setter = (BESMQMessage.PropertySetter)JMS_PROPERTY_SETERS.get(name);
            if (setter != null && value != null) {
                setter.set(this, value);
            } else {
                try {
                    this.setProperty(name, value);
                } catch (IOException var6) {
                    throw JMSExceptionSupport.create(var6);
                }
            }

        } else {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }
    }

    public void setProperties(Map<String, ?> properties) throws JMSException {
        Iterator i$ = properties.entrySet().iterator();

        while(i$.hasNext()) {
            Entry<String, ?> entry = (Entry)i$.next();
            this.setObjectProperty((String)entry.getKey(), entry.getValue());
        }

    }

    protected void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character || value instanceof String || value == null;
        if (!valid) {
            BESMQConnection conn = this.getConnection();
            if (conn != null && !conn.isNestedMapAndListEnabled()) {
                throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
            }

            if (!(value instanceof Map) && !(value instanceof List)) {
                throw new MessageFormatException("Only objectified primitive objects, String, Map and List types are allowed but was: " + value + " type: " + value.getClass());
            }
        }

    }

    protected void checkValidScheduled(String name, Object value) throws MessageFormatException {
        if (("BMQ_SCHEDULED_DELAY".equals(name) || "BMQ_SCHEDULED_PERIOD".equals(name) || "BMQ_SCHEDULED_REPEAT".equals(name)) && !(value instanceof Long) && !(value instanceof Integer)) {
            throw new MessageFormatException(name + " should be long or int value");
        } else {
            if ("BMQ_SCHEDULED_CRON".equals(name)) {
                CronParser.validate(value.toString());
            }

        }
    }

    protected Object convertScheduled(String name, Object value) throws MessageFormatException {
        Object result = value;
        if ("BMQ_SCHEDULED_DELAY".equals(name)) {
            result = TypeConversionSupport.convert(value, Long.class);
        } else if ("BMQ_SCHEDULED_PERIOD".equals(name)) {
            result = TypeConversionSupport.convert(value, Long.class);
        } else if ("BMQ_SCHEDULED_REPEAT".equals(name)) {
            result = TypeConversionSupport.convert(value, Integer.class);
        }

        return result;
    }

    public Object getObjectProperty(String name) throws JMSException {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        } else {
            PropertyExpression expression = new PropertyExpression(name);
            return expression.evaluate(this);
        }
    }

    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            return false;
        } else {
            Boolean rc = (Boolean)TypeConversionSupport.convert(value, Boolean.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
            } else {
                return rc;
            }
        }
    }

    public byte getByteProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        } else {
            Byte rc = (Byte)TypeConversionSupport.convert(value, Byte.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
            } else {
                return rc;
            }
        }
    }

    public short getShortProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        } else {
            Short rc = (Short)TypeConversionSupport.convert(value, Short.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
            } else {
                return rc;
            }
        }
    }

    public int getIntProperty(String name) throws JMSException {
        if (name.equals("JMSXGroupSeq")) {
            return this.getGroupSequence();
        } else if (name.equals("JMSXDeliveryCount")) {
            return this.getRedeliveryCounter() + 1;
        } else {
            Object value = this.getObjectProperty(name);
            if (value == null) {
                throw new NumberFormatException("property " + name + " was null");
            } else {
                Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
                } else {
                    return rc;
                }
            }
        }
    }

    public long getLongProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        } else {
            Long rc = (Long)TypeConversionSupport.convert(value, Long.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
            } else {
                return rc;
            }
        }
    }

    public float getFloatProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        } else {
            Float rc = (Float)TypeConversionSupport.convert(value, Float.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
            } else {
                return rc;
            }
        }
    }

    public double getDoubleProperty(String name) throws JMSException {
        Object value = this.getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        } else {
            Double rc = (Double)TypeConversionSupport.convert(value, Double.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
            } else {
                return rc;
            }
        }
    }

    public String getStringProperty(String name) throws JMSException {
        Object value = null;
        if (name.equals("JMSXUserID")) {
            value = this.getUserID();
            if (value == null) {
                value = this.getObjectProperty(name);
            }
        } else if (name.equals("JMSXGroupID")) {
            value = this.getGroupID();
            if (value == null) {
                value = this.getObjectProperty(name);
            }
        } else {
            value = this.getObjectProperty(name);
        }

        if (value == null) {
            return null;
        } else {
            String rc = (String)TypeConversionSupport.convert(value, String.class);
            if (rc == null) {
                throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
            } else {
                return rc;
            }
        }
    }

    public void setBooleanProperty(String name, boolean value) throws JMSException {
        this.setBooleanProperty(name, value, true);
    }

    public void setBooleanProperty(String name, boolean value, boolean checkReadOnly) throws JMSException {
        this.setObjectProperty(name, value, checkReadOnly);
    }

    public void setByteProperty(String name, byte value) throws JMSException {
        this.setObjectProperty(name, value);
    }

    public void setShortProperty(String name, short value) throws JMSException {
        this.setObjectProperty(name, value);
    }

    public void setIntProperty(String name, int value) throws JMSException {
        this.setObjectProperty(name, value);
    }

    public void setLongProperty(String name, long value) throws JMSException {
        this.setObjectProperty(name, value, true);
    }

    public void setFloatProperty(String name, float value) throws JMSException {
        this.setObjectProperty(name, new Float(value));
    }

    public void setDoubleProperty(String name, double value) throws JMSException {
        this.setObjectProperty(name, new Double(value));
    }

    public void setStringProperty(String name, String value) throws JMSException {
        this.setObjectProperty(name, value);
    }

    private void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (this.readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (this.readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    public Callback getAcknowledgeCallback() {
        return this.acknowledgeCallback;
    }

    public void setAcknowledgeCallback(Callback acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }

    public void onSend() throws JMSException {
        this.setReadOnlyBody(true);
        this.setReadOnlyProperties(true);
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processMessage(this);
    }

    public void storeContent() {
    }

    public void beforeMarshall(ProtocolFormat protocolFormat) throws IOException {
        BESMQConnection connection = this.getConnection();
        if (connection != null && connection.isUseEncryption() && !this.isEncrypted()) {
            this.setEncrypted(true);
        }

        super.beforeMarshall(protocolFormat);
    }

    protected ByteSequence encrypt(ByteSequence content) throws JMSException {
        BESMQConnection connection = this.getConnection();
        if (connection != null && connection.isUseEncryption()) {
            content = connection.getMessageEncryptor().encrypt(content);
        }

        return content;
    }

    protected ByteSequence decrypt(ByteSequence content) throws JMSException {
        try {
            if (this.isEncrypted()) {
                BESMQConnection connection = this.getConnection();
                if (connection != null && connection.isUseEncryption()) {
                    content = connection.getMessageEncryptor().decrypt(content);
                } else if (this.isCompressed()) {
                    throw new JMSException("Message has compressed and encrypted, can't only uncompress it here");
                }
            }

            this.setEncrypted(false);
            return content;
        } catch (IOException var3) {
            throw JMSExceptionSupport.create("Failed to decrypt message", var3);
        }
    }

    static {
        JMS_PROPERTY_SETERS.put("JMSXDeliveryCount", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    message.setRedeliveryCounter(rc - 1);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupID", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String)TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupID cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    message.setGroupID(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupSeq", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupSeq cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    message.setGroupSequence(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSCorrelationID", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String)TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSCorrelationID cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSCorrelationID(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSDeliveryMode", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    Boolean bool = (Boolean)TypeConversionSupport.convert(value, Boolean.class);
                    if (bool == null) {
                        throw new MessageFormatException("Property JMSDeliveryMode cannot be set from a " + value.getClass().getName() + ".");
                    }

                    rc = bool ? 2 : 1;
                }

                ((BESMQMessage)message).setJMSDeliveryMode(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSExpiration", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Long rc = (Long)TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSExpiration cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSExpiration(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSPriority", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSPriority cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSPriority(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSRedelivered", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Boolean rc = (Boolean)TypeConversionSupport.convert(value, Boolean.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSRedelivered cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSRedelivered(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSReplyTo", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                BESMQDestination rc = (BESMQDestination)TypeConversionSupport.convert(value, BESMQDestination.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSReplyTo cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setReplyTo(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSTimestamp", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                Long rc = (Long)TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSTimestamp cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSTimestamp(rc);
                }
            }
        });
        JMS_PROPERTY_SETERS.put("JMSType", new BESMQMessage.PropertySetter() {
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String)TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSType cannot be set from a " + value.getClass().getName() + ".");
                } else {
                    ((BESMQMessage)message).setJMSType(rc);
                }
            }
        });
    }

    interface PropertySetter {
        void set(Message var1, Object var2) throws MessageFormatException;
    }
}
