package org.ling.zookeeper;

import jline.console.ConsoleReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by link on 2017/7/30.
 */
public class MultiZk {
    private static final Logger logger = LoggerFactory.getLogger(MultiZk.class);
    private List<ZookeeperThread> zookeeperThreads;
    private int numThreads = 20;
    private int intervalMs = 5000;
    private String urls = "localhost:2181";
    private String state = "Stopped";
    private ConsoleReader reader;


    public boolean init(String[] args) {
        try {
            reader = new ConsoleReader();
        } catch (IOException e) {
            logger.error("Unable to create a console reader.", e);
            return false;
        }
        numThreads = 20;
        intervalMs = 5000;
        urls = "localhost:2181";
        state = "Stopped";
        zookeeperThreads = new ArrayList<ZookeeperThread>();

        return true;
    }

    private String buildPrompt() {
        return String.format("[ %s | Threads: %s, Interval: %s, Urls: %s ]\n>> ", state, numThreads, intervalMs, urls);
    }

    private void startCmd() {
        if ("Running".equals(state)) {
            System.out.println("All threads are already running now.");
            return;
        }
        System.out.println("Starting...");
        for (int i = 0; i < numThreads; i++) {
            try {
                ZookeeperThread zkThread = new ZookeeperThread(urls, intervalMs);
                zkThread.run();
                zookeeperThreads.add(zkThread);
            } catch (IOException e) {
                System.out.println(e.toString());
                logger.error("Unable to create a zookeeper instance.", e);
            }
        }
        state = "Running";

    }

    private void stopCmd() {
        if ("Stopped".equals(state)) {
            System.out.println("All threads are stopped");
            return;
        }
        System.out.println("Stopping...");
        for (ZookeeperThread thread : zookeeperThreads) {
            thread.close();
        }
        state = "Stopped";
    }

    private void quitCmd() {
        stopCmd();
        System.out.println("Exit ...");
        System.exit(0);
    }

    private boolean checkNotNull(String str) {
        if (str != null && !str.equals("")) {
            return true;
        }
        return false;
    }

    private void setCmd() throws IOException {
        String newThreads = reader.readLine("New number of threads: ");
        if (checkNotNull(newThreads)) {
            if (newThreads.matches("\\d+")) {
                numThreads = Integer.valueOf(newThreads);
            } else {
                System.out.println(String.format("The %s is not a number.", newThreads));
            }
        }
        String newInterval = reader.readLine("Interval(ms) to action on zookeeper: ");
        if (checkNotNull(newInterval)) {
            if (newInterval.matches("\\d+")) {
                intervalMs = Integer.valueOf(newInterval);
            } else {
                System.out.println(String.format("The %s is not a number.", newInterval));
            }
        }

        String newUrls = reader.readLine("New urls of zookeeper: ");
        if (checkNotNull(newUrls)) {
            urls = newUrls;
        }
    }

    private void helpCmd() {

    }

    private void executeCommand(String cmd) {
        if ("start".equals(cmd)) {
            startCmd();
        } else if ("stop".equals(cmd)) {
            stopCmd();
        } else if ("set".equals(cmd)) {
            try {
                setCmd();
            } catch (IOException e) {
                logger.error("Can't set property.", e);
            }

        } else if ("help".equals(cmd)) {

        } else if ("exit".equals(cmd) || "quit".equals(cmd)) {
            quitCmd();
        }
    }

    public void run() throws IOException {
        String cmd;
        while ((cmd = reader.readLine(buildPrompt())) != null) {
            executeCommand(cmd.trim().toLowerCase());
        }
    }

    public static void main(String[] args) {
        MultiZk app = new MultiZk();
        if (app.init(args)) {
            try {
                app.run();
            } catch (IOException e) {
                logger.error("Read line error", e);
            }
        }
    }

    private static class ZookeeperThread extends Thread {
        private static final Logger logger = LoggerFactory.getLogger(ZookeeperThread.class);

        private String urls;
        private int interval;
        private ZooKeeper zookeeper;

        public ZookeeperThread(String urls, int interval) throws IOException {
            this.urls = urls;
            this.interval = interval;
            this.zookeeper = new ZooKeeper(urls, 5000, null);
        }

        public ZookeeperThread(String urls) throws IOException {
            this(urls, 5000);
        }

        public ZookeeperThread() throws IOException {
            this("localhost:2181");
        }

        @Override
        public void run() {
            super.run();
            try {
//                while (zookeeper != null && zookeeper.getState().isAlive()) {
                    Thread.sleep(interval);
                    zookeeper.exists("/", new Watcher() {
                        public void process(WatchedEvent watchedEvent) {

                        }
                    });
//                }
            } catch (InterruptedException e) {
                System.out.println(e.toString());
                logger.error("Unknown error happened", e);
            } catch (KeeperException e) {
                System.out.println(e.toString());
                logger.error("No znode error", e);
            }
        }

        public void close() {
            try {
                if (zookeeper != null) {
                    zookeeper.close();
                }
            } catch (InterruptedException e) {
                logger.error("Unable to close the zookeeper client.", e);
            }
        }

    }
}
