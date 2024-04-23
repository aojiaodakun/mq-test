//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.bes.mq.command;

import com.bes.mq.util.IntrospectionSupport;

import java.io.Serializable;
import java.util.Map;

public abstract class BaseCommand implements Command, Serializable {
    protected int commandId;
    protected boolean responseRequired;
    private transient Endpoint from;
    private transient Endpoint to;

    public BaseCommand() {
    }

    public void copy(BaseCommand copy) {
        copy.commandId = this.commandId;
        copy.responseRequired = this.responseRequired;
    }

    public int getCommandId() {
        return this.commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public boolean isResponseRequired() {
        return this.responseRequired;
    }

    public void setResponseRequired(boolean responseRequired) {
        this.responseRequired = responseRequired;
    }

    public String toString() {
        return this.toString((Map)null);
    }

    public String toString(Map<String, Object> overrideFields) {
        return IntrospectionSupport.toString(this, BaseCommand.class, overrideFields);
    }

    public boolean isProtocolFormatInfo() {
        return false;
    }

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isResponse() {
        return false;
    }

    public boolean isMessageDispatch() {
        return false;
    }

    public boolean isMessage() {
        return false;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public boolean isMessageAck() {
        return false;
    }

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }

    public boolean isConnectionControl() {
        return false;
    }

    public Endpoint getFrom() {
        return this.from;
    }

    public void setFrom(Endpoint from) {
        this.from = from;
    }

    public Endpoint getTo() {
        return this.to;
    }

    public void setTo(Endpoint to) {
        this.to = to;
    }
}
