package com.zeroclue.jmeter.protocol.amqp;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


public class ConcurrentFileLogger {
    private static final ConcurrentHashMap<String, ConcurrentFileLogger> instances = new ConcurrentHashMap<>();
    private final String filePath;

    private FileChannel fileChannel;
    private final ReentrantLock lock = new ReentrantLock();
    private final String baseFileName;
    private final String fileExtension;
    private final Long sizeLimit;
    private String currentDate;
    private File currentFile;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    private ConcurrentFileLogger(String filePath, Long sizeLimit) throws IOException {
        //this.baseFileName = filePath;
        this.filePath = filePath;
        this.baseFileName = getFileNameWithoutExtension(filePath);
        this.fileExtension = getFullExtension(filePath);
        if (sizeLimit == null) {
            sizeLimit = Long.valueOf(20 * 1024 * 1024);
        }
        this.sizeLimit = sizeLimit;
        this.rollOver();
    }

    private void rollOver() throws IOException {
        // 获取当前日期
        String newDate = DATE_FORMAT.format(new Date());
        if (!newDate.equals(currentDate)) {
            currentDate = newDate; // 更新日期
        }

        if (fileChannel != null) {
            fileChannel.force(true);
            fileChannel.close();
        }
        this.currentFile = new File(filePath);
        File parentDir = currentFile.getParentFile();

        // 如果父目录不存在，则创建它
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        if(this.currentFile.exists() && this.currentFile.length() > sizeLimit) {
            // 获取文件的父目录
            String currentLogFile = getLogFileName();
            try {
                if (this.currentFile.exists())
                    Files.move(this.currentFile.toPath(), Paths.get(currentLogFile));
            } catch (IOException e) {
                log(e.getMessage());
            }
        }
        this.currentFile = new File(filePath);
        fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
    }


    private final List<Object> requesters = new LinkedList<>();

    public static ConcurrentFileLogger getInstance(String filePath, Long sizeLimit, Object requester) throws IOException {
        // 从字典中获取已存在的实例
        if (sizeLimit == null) {
            sizeLimit = (long) (20 * 1024 * 1024);
        }
        ConcurrentFileLogger logger = instances.get(filePath);
        if (logger == null) {
            // 如果字典中没有，创建一个新的并加入字典
            synchronized (ConcurrentFileLogger.class) {
                logger = instances.get(filePath);
                if (logger == null) {
                    logger = new ConcurrentFileLogger(filePath, sizeLimit);
                    instances.put(filePath, logger);
                }
            }
        }
        synchronized (logger.requesters) {
            logger.requesters.add(requester);
            logger.rollOver();
        }
        return logger;
    }

    public void log(String message) {
        lock.lock();
        try {
            // 检查是否需要切换文件
            if (currentFile.length() > this.sizeLimit) {
                rollOver();
            }
            ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes());
            fileChannel.write(buffer);
            fileChannel.force(true);
        } catch (Exception e) {
            this.log(e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public static String getFullExtension(String fileName) {
        if (fileName == null || !fileName.contains(".")) {
            return "";
        }
        return fileName.substring(fileName.indexOf('.') + 1);
    }

    public static String getFileNameWithoutExtension(String fileName) {
        if (fileName == null || !fileName.contains(".")) {
            return fileName; // 没有扩展名，直接返回
        }
        return fileName.substring(0, fileName.lastIndexOf('.'));
    }

    private String getLogFileName() {
        String baseName = this.baseFileName + "." + currentDate;
        int count = 1;

        String logFile = baseName + "." + count + "." + fileExtension;
        // 如果文件存在且超过大小，则寻找下一个可用序号
        while (Files.exists(Paths.get(logFile))) {
            count++;
            logFile = baseName + "." + count + "." + fileExtension;
        }
        return logFile;
    }


    public void close(Object requester) throws IOException {
        synchronized (requesters) {
            requesters.remove(requester);
            if (requesters.isEmpty()) {
                fileChannel.close();
                instances.remove(filePath);
            }
        }
    }

}
