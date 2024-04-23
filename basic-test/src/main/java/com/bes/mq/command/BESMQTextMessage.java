//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.bes.mq.command;

import com.bes.mq.BESMQConnection;
import com.bes.mq.protocolformat.ProtocolFormat;
import com.bes.mq.util.ByteArrayInputStream;
import com.bes.mq.util.ByteArrayOutputStream;
import com.bes.mq.util.ByteSequence;
import com.bes.mq.util.JMSExceptionSupport;
import com.bes.mq.util.MarshallingSupport;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

public class BESMQTextMessage extends BESMQMessage implements TextMessage, Serializable {
    public static final byte DATA_STRUCTURE_TYPE = 39;
    protected String text;

    public BESMQTextMessage() {
    }

    public BESMQTextMessage(int i) {
    }

    public Message copy() {
        BESMQTextMessage copy = new BESMQTextMessage();
        this.copy(copy);
        return copy;
    }

    private void copy(BESMQTextMessage copy) {
        super.copy(copy);
        copy.text = this.text;
    }

    public byte getDataStructureType() {
        return 39;
    }

    public String getJMSXMimeType() {
        return "jms/text-message";
    }

    public void setText(String text) throws MessageNotWriteableException {
        this.checkReadOnlyBody();
        this.text = text;
        this.setContent((ByteSequence)null);
    }

    public String getText() throws JMSException {
        if (this.text == null && this.getContent() != null) {
            Object is = null;

            try {
                ByteSequence bodyAsBytes = this.getContent();
                if (bodyAsBytes != null) {
                    bodyAsBytes = this.decrypt(bodyAsBytes);
                    is = new ByteArrayInputStream(bodyAsBytes);
                    if (this.isCompressed()) {
                        is = new InflaterInputStream((InputStream)is);
                    }

                    DataInputStream dataIn = new DataInputStream((InputStream)is);
                    this.text = MarshallingSupport.readUTF8(dataIn);
                    dataIn.close();
                    this.setContent((ByteSequence)null);
                    this.setCompressed(false);
                }
            } catch (IOException var11) {
                throw JMSExceptionSupport.create(var11);
            } finally {
                if (is != null) {
                    try {
                        ((InputStream)is).close();
                    } catch (IOException var10) {
                    }
                }

            }
        }

        return this.text;
    }

    public void beforeMarshall(ProtocolFormat protocolFormat) throws IOException {
        super.beforeMarshall(protocolFormat);
        this.storeContent();
    }

    public void storeContent() {
        try {
            ByteSequence content = this.getContent();
            if (content == null && this.text != null) {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                OutputStream os = bytesOut;
                BESMQConnection connection = this.getConnection();
                if (connection != null && connection.isUseCompression()) {
                    this.compressed = true;
                    os = new DeflaterOutputStream(bytesOut);
                }

                DataOutputStream dataOut = new DataOutputStream((OutputStream)os);
                MarshallingSupport.writeUTF8(dataOut, this.text);
                dataOut.close();
                content = this.encrypt(bytesOut.toByteSequence());
                this.setContent(content);
            }

        } catch (Exception var6) {
            throw new RuntimeException(var6);
        }
    }

    public void clearMarshalledState() throws JMSException {
        super.clearMarshalledState();
        this.text = null;
    }

    public void clearBody() throws JMSException {
        super.clearBody();
        this.text = null;
    }

    public int getSize() {
        if (this.size == 0 && this.content == null && this.text != null) {
            this.size = this.getMinimumMessageSize();
            if (this.marshalledProperties != null) {
                this.size += this.marshalledProperties.getLength();
            }

            this.size += this.text.length() * 2;
        }

        return super.getSize();
    }

    public String toString() {
        try {
            String text = this.getText();
            if (text != null) {
                text = MarshallingSupport.truncate64(text);
                HashMap<String, Object> overrideFields = new HashMap();
                overrideFields.put("text", text);
                return super.toString(overrideFields);
            }
        } catch (JMSException var3) {
        }

        return super.toString();
    }
}
