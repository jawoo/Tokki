package org.cgiar.tokki;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Terminal progress bar for long-running batch work. Uses Unicode block characters
 * when attached to a console; otherwise prints occasional percentage lines.
 */
public final class ConsoleProgress
{
    private final String label;
    private final long total;
    private final AtomicLong done = new AtomicLong(0);
    private final boolean graphical;
    private final PrintStream out;
    private boolean completedLine;
    private static final int BAR_WIDTH = 28;

    public ConsoleProgress(String label, long total)
    {
        this(label, total, System.out);
    }

    ConsoleProgress(String label, long total, PrintStream out)
    {
        this.label = label;
        this.total = Math.max(1L, total);
        this.out = out;
        this.graphical = out == System.out && System.console() != null;
    }

    /** Call once after each completed step (e.g. one DSSAT invocation). Thread-safe. */
    public void step()
    {
        long c = done.incrementAndGet();
        long cap = Math.min(c, total);
        if (graphical)
        {
            synchronized (this)
            {
                draw(cap);
            }
        }
        else
        {
            long step = Math.max(1L, total / 40L);
            if (c == total || c % step == 0)
            {
                synchronized (this)
                {
                    int pct = (int) Math.min(100L, (100L * cap) / total);
                    out.println("> " + label + " " + pct + "% (" + cap + "/" + total + ")");
                }
            }
        }
    }

    private void draw(long cap)
    {
        int pct = (int) ((100L * cap) / total);
        int filled = (int) ((BAR_WIDTH * cap) / total);
        StringBuilder bar = new StringBuilder(BAR_WIDTH);
        for (int i = 0; i < BAR_WIDTH; i++)
        {
            bar.append(i < filled ? '\u2588' : '\u2591');
        }
        out.print("\r> " + label + " [" + bar + "] " + pct + "% (" + cap + "/" + total + ")  ");
        out.flush();
        if (cap >= total && !completedLine)
        {
            out.println();
            completedLine = true;
        }
    }

    /** If the bar never reached 100% (count mismatch), close the line. */
    public void finish()
    {
        if (!graphical)
        {
            return;
        }
        synchronized (this)
        {
            if (!completedLine)
            {
                draw(Math.min(done.get(), total));
                if (!completedLine)
                {
                    out.println();
                    completedLine = true;
                }
            }
        }
    }
}
