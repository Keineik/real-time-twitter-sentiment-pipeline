package hcmus.group02.Bolt;

import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class SentimentBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentBolt.class);
    private OutputCollector collector;
    private final String filepath;
    private Map<String, Integer> sentimentMap;

    public SentimentBolt(String filepath) {
        String resourcesPath = "src/main/resources/";
        this.filepath = resourcesPath + filepath;
    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.sentimentMap = new HashMap<>();

        // Read the AFINN sentiment file and stores the key, value pairs to a Map
        try {
            BufferedReader br = new BufferedReader(new FileReader(filepath));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                sentimentMap.put(tokens[0], Integer.parseInt(tokens[1]));
            }
            br.close();
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
    }

    public void execute(Tuple tuple) {
        try {
            String tweetId = tuple.getStringByField("tweet_id");
            String tweet = tuple.getStringByField("tweet");
            int sentimentScore = getSentimentScore(tweet);
            // print debug
            System.out.printf("%s has sentiment score of: %s%n", tweetId, sentimentScore);
            collector.emit(new Values(tweetId, sentimentScore));
            collector.ack(tuple);
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage());
            collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "sentiment_score"));
    }

    private int getSentimentScore(String tweet) {
        // Remove all punctuation and new line chars in the tweet.
        tweet = tweet.replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
        // Split the tweet to tokens
        Iterable<String> tokens = Splitter.on(" ").trimResults().omitEmptyStrings().split(tweet);
        // Score the tweet
        int sentimentPoint = 0;
        for (String token : tokens) {
            if (sentimentMap.containsKey(token)) {
                sentimentPoint += sentimentMap.get(token);
            }
        }
        return sentimentPoint;
    }
}
