package hcmus.group02.Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.min;

public class TrendWindowingBolt extends BaseWindowedBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrendWindowingBolt.class);
    private OutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "tweet_cnt", "rank", "window_start", "window_end"));
    }

    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow window) {
        Map<String, Integer> hashtagCounts = new HashMap<>();

        LOGGER.warn("Processing window with {} tuples, start: {}, end: {}",
                window.get().size(), window.getStartTimestamp(), window.getEndTimestamp());

        for (var tuple : window.get()) {
            String tweet = tuple.getStringByField("tweet");
            var hashtags = extractHashtags(tweet);
            for (String hashtag : hashtags) {
                hashtagCounts.put(hashtag, hashtagCounts.getOrDefault(hashtag, 0) + 1);
            }
        }

        // Get timestamp information for this window
        long windowStart = window.getStartTimestamp();
        long windowEnd = window.getEndTimestamp();

        // Get top ranking tweet
        PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                (a, b) -> b.getValue().compareTo(a.getValue()));
        pq.addAll(hashtagCounts.entrySet());

        // Extract top 20 hashtags
        int limit = min(20, pq.size());
        for (int rank = 1; rank <= limit; rank++) {
            var entry = pq.poll();
            if (entry != null) {
                String hashtag = entry.getKey();
                int count = entry.getValue();

                // Emit as tuple
                collector.emit(new Values(hashtag, count, rank,
                        new Timestamp(windowStart), new Timestamp(windowEnd)));
            }
        }
    }

    public Set<String> extractHashtags(String tweetText) {
        Set<String> hashtags = new HashSet<>();

        // This regex matches Twitter hashtags: # followed by letters, numbers, or underscores
        // (\\w+) - Captures one or more "word characters" in a group:
        // \\w represents any letter, digit, or underscore [a-zA-Z0-9_]
        // + means "one or more" of these characters
        Pattern pattern = Pattern.compile("#(\\w+)");
        Matcher matcher = pattern.matcher(tweetText);
        while (matcher.find()) {
            hashtags.add(matcher.group(0).toLowerCase());
        }
        return hashtags;
    }
}
