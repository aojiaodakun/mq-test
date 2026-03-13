package org.apache.rocketmq.logging.org.slf4j;

public class Slf4jDelegateLogger implements Logger {

    private final org.slf4j.Logger delegate;

    public Slf4jDelegateLogger(org.slf4j.Logger logger) {
        this.delegate = logger;
    }

    private org.slf4j.Marker map(Marker marker) {
        if (marker == null) return null;
        return org.slf4j.MarkerFactory.getMarker(marker.getName());
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    /* ---------- TRACE ---------- */

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(map(marker));
    }

    @Override
    public void trace(String msg) {
        delegate.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
        delegate.trace(format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        delegate.trace(format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        delegate.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        delegate.trace(msg, t);
    }

    @Override
    public void trace(Marker marker, String msg) {
        delegate.trace(map(marker), msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        delegate.trace(map(marker), format, arg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        delegate.trace(map(marker), format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... arguments) {
        delegate.trace(map(marker), format, arguments);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        delegate.trace(map(marker), msg, t);
    }


    /* ---------- DEBUG ---------- */

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(map(marker));
    }

    @Override
    public void debug(String msg) {
        delegate.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
        delegate.debug(format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        delegate.debug(format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        delegate.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        delegate.debug(msg, t);
    }

    @Override
    public void debug(Marker marker, String msg) {
        delegate.debug(map(marker), msg);
    }

    @Override
    public void debug(Marker marker, String msg, Object arg) {
        delegate.debug(map(marker), msg, arg);
    }

    @Override
    public void debug(Marker marker, String msg, Object arg1, Object arg2) {
        delegate.debug(map(marker), msg, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String msg, Object... arguments) {
        delegate.debug(map(marker), msg, arguments);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        delegate.debug(map(marker), msg, t);
    }


    /* ---------- INFO ---------- */

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(map(marker));
    }

    @Override
    public void info(String msg) {
        delegate.info(msg);
    }

    @Override
    public void info(String format, Object arg) {
        delegate.info(format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        delegate.info(format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        delegate.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        delegate.info(msg, t);
    }

    @Override
    public void info(Marker marker, String msg) {
        delegate.info(map(marker), msg);
    }

    @Override
    public void info(Marker marker, String msg, Object arg) {
        delegate.info(map(marker), msg, arg);
    }

    @Override
    public void info(Marker marker, String msg, Object arg1, Object arg2) {
        delegate.info(map(marker), msg, arg1, arg2);
    }

    @Override
    public void info(Marker marker, String msg, Object... arguments) {
        delegate.info(map(marker), msg, arguments);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        delegate.info(map(marker), msg, t);
    }


    /* ---------- WARN ---------- */

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(map(marker));
    }

    @Override
    public void warn(String msg) {
        delegate.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
        delegate.warn(format, arg);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        delegate.warn(format, arg1, arg2);
    }

    @Override
    public void warn(String format, Object... arguments) {
        delegate.warn(format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
        delegate.warn(msg, t);
    }

    @Override
    public void warn(Marker marker, String msg) {
        delegate.warn(map(marker), msg);
    }

    @Override
    public void warn(Marker marker, String msg, Object arg) {
        delegate.warn(map(marker), msg, arg);
    }

    @Override
    public void warn(Marker marker, String msg, Object arg1, Object arg2) {
        delegate.warn(map(marker), msg, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String msg, Object... arguments) {
        delegate.warn(map(marker), msg, arguments);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        delegate.warn(map(marker), msg, t);
    }


    /* ---------- ERROR ---------- */

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(map(marker));
    }

    @Override
    public void error(String msg) {
        delegate.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
        delegate.error(format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        delegate.error(format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        delegate.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        delegate.error(msg, t);
    }

    @Override
    public void error(Marker marker, String msg) {
        delegate.error(map(marker), msg);
    }

    @Override
    public void error(Marker marker, String msg, Object arg) {
        delegate.error(map(marker), msg, arg);
    }

    @Override
    public void error(Marker marker, String msg, Object arg1, Object arg2) {
        delegate.error(map(marker), msg, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String msg, Object... arguments) {
        delegate.error(map(marker), msg, arguments);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        delegate.error(map(marker), msg, t);
    }
}
