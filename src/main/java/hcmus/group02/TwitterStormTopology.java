package hcmus.group02;

import hcmus.group02.Bolt.JsonParsingBolt;
import hcmus.group02.Bolt.PostgresBolt;
import hcmus.group02.Bolt.SentimentBolt;
import hcmus.group02.Bolt.StateCountingBolt;
import hcmus.group02.Spout.TwitterFileListeningSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


public class TwitterStormTopology
{
    private static final int ONE_MINUTES = 60000;

    public static void main( String[] args ) throws Exception {
        String fields = "created_at,tweet_id,tweet,likes,retweet_count,source," +
                "user_id,user_name,user_screen_name,user_join_date,user_followers_count," +
                "city,country,state,state_code,collected_at";

        TopologyBuilder builder = new TopologyBuilder();

        // Fake stream from file
        builder.setSpout("TwitterFileListeningSpout", new TwitterFileListeningSpout("data/hashtag_joebiden.json"));
//        // Take input from Kafka
//        builder.setSpout("KafkaSpout", new KafkaSpout<>(KafkaSpoutConfig.builder(
//                "localhost:9092", "twitter-kafka-stream").build()));

        // Parse JSON
        builder.setBolt("JsonParsingBolt", new JsonParsingBolt(fields))
                .shuffleGrouping("TwitterFileListeningSpout");

        // Count tweet group by state
        builder.setBolt("StateCountingBolt", new StateCountingBolt())
                .fieldsGrouping("JsonParsingBolt", new Fields("state"));

        // Persist data to database
        builder.setBolt("PostgresBolt", new PostgresBolt(fields, "new table"))
                .shuffleGrouping("JsonParsingBolt");

        // Get sentiment of tweets
        builder.setBolt("SentimentBolt", new SentimentBolt("AFINN-en-165.txt"))
                .shuffleGrouping("PostgresBolt");

        // Update sentiment to database
        builder.setBolt("SentimentUpdatingBolt", new PostgresBolt("", ""))
                .shuffleGrouping("SentimentBolt");

        // Update tweet count by state to database
        builder.setBolt("StateCountPersistingBolt", new PostgresBolt("", ""))
                .shuffleGrouping("StateCountingBolt");

        localSubmitter(builder.createTopology());
    }

    private static void localSubmitter(StormTopology topology) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        try (LocalCluster cluster = new LocalCluster()) { // Try-with-resources
            cluster.submitTopology("LocalStormTopology", config, topology);
            Utils.sleep(ONE_MINUTES/6);
            cluster.killTopology("LocalStormTopology");
            cluster.shutdown();
        }
    }

    private static void clusterSubmitter(StormTopology topology) throws Exception {
        Config config = new Config();
        config.setNumWorkers(5);
        config.setMaxSpoutPending(500);
        StormSubmitter.submitTopology("twitter-topology", config, topology);
    }
}
