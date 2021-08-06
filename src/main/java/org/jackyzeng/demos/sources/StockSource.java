package org.jackyzeng.demos.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.jackyzeng.demos.utils.StockPrice;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockSource implements SourceFunction<StockPrice> {

    private Boolean isRunning = true;
    private String path;
    private InputStream streamSource;

    public StockSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> sourceContext) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(streamSource));

        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(split[1] + " " + split[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            if (isFirstLine) {
                lastEventTs = eventTs;
                isFirstLine = false;
            }

            StockPrice stock = StockPrice.builder().symbol(split[0]).timestamp(eventTs).volume(Integer.parseInt(split[4])).build();
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0) {
                Thread.sleep(timeDiff);
            }
            sourceContext.collect(stock);
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
