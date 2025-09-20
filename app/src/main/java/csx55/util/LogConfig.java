package csx55.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.*;

/*
 * 
 * This class was initially generated entirely by ChatGPT (and edited by me) just to provide better formatted logs using the java logging package. 
 * It does not include anything related to the actual assignment or provide assistance with the assigment.
 * I'm using it to replace standard print calls.
 * 
 */

public final class LogConfig {
    @SuppressWarnings("unused")
    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneId.systemDefault());

    /** Simple formatter: time, level, thread, class.method, message, and stack trace if present. */
    public static class CompactFormatter extends Formatter {
        @SuppressWarnings("unused")
        @Override
        public String format(LogRecord r) {
            String src = r.getLoggerName(); // e.g., csx55.overlay.node.MessagingNode[1.2.3.4:5000]

            // Separate FQN class and the suffix (e.g., "[1.2.3.4:5000]")
            int bracket = src.indexOf('[');
            String fqn = (bracket >= 0) ? src.substring(0, bracket) : src;
            String suffix = (bracket >= 0) ? src.substring(bracket) : "";

            // Take the simple class name from the FQN
            int lastDot = fqn.lastIndexOf('.');
            String simple = (lastDot >= 0 && lastDot + 1 < fqn.length()) ? fqn.substring(lastDot + 1) : fqn;

            // Final short name like: MessagingNode[1.2.3.4:5000]
            String shortName = simple + suffix;
            String method = (r.getSourceMethodName() != null) ? r.getSourceMethodName() : "";
            String threadName = Thread.currentThread().getName();   // name; r.getThreadID() is JUL’s int id
            String msg = formatMessage(r);

            StringBuilder sb = new StringBuilder()
            .append(String.format("%-7s", r.getLevel().getName())).append(' ')
            .append(shortName).append(" - ")
            .append(msg).append('\n');

            if (r.getThrown() != null) {
                StringWriter sw = new StringWriter();
                r.getThrown().printStackTrace(new PrintWriter(sw));
                sb.append(sw);
            }
            return sb.toString();
        }
    }

    /** Call once at startup (e.g., at the top of main). */
    public static void init(Level rootLevel) {
        Logger root = Logger.getLogger("");
        root.setLevel(rootLevel);

        // Remove default handlers (optional—keeps output predictable)
        for (Handler h : root.getHandlers()) root.removeHandler(h);

        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);                 // handler threshold
        ch.setFormatter(new CompactFormatter()); // include class/method/thread
        root.addHandler(ch);

        // Optional: add a file handler too
        // try {
        //     FileHandler fh = new FileHandler("messaging-node.log", true);
        //     fh.setLevel(Level.FINE);
        //     fh.setFormatter(new CompactFormatter());
        //     root.addHandler(fh);
        // } catch (Exception e) {
        //     Logger.getLogger(LogConfig.class.getName()).log(Level.WARNING, "File handler setup failed", e);
        // }
    }
}

