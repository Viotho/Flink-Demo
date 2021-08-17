package org.jackyzeng.demos.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.jackyzeng.demos.utils.UserBehaviour;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class UserBehaviourSource implements SourceFunction<UserBehaviour> {

    private boolean isRunning = true;
    private final String path;
    private InputStream streamSource;

    public UserBehaviourSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<UserBehaviour> sourceContext) throws Exception {
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            long eventTs = Long.parseLong(itemStrArr[4]);
            if (isFirstLine) {
                // 从第一行数据提取时间戳
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            UserBehaviour userBehavior = UserBehaviour.builder().userId(Long.parseLong(itemStrArr[0]))
                    .itemId(Long.parseLong(itemStrArr[1]))
                    .categoryId(Integer.parseInt(itemStrArr[2]))
                    .behaviour(itemStrArr[3])
                    .timestamp(eventTs)
                    .build();
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0)
                Thread.sleep(timeDiff * 1000);
            sourceContext.collect(userBehavior);
            lastEventTs = eventTs;
        }
    }

    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        isRunning = false;
    }

}
