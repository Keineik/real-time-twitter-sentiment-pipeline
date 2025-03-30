package hcmus.group02;

import hcmus.group02.Bolt.PostgresBolt;
import hcmus.group02.Bolt.SentimentBolt;
import hcmus.group02.Bolt.StateCountingBolt;
import hcmus.group02.Bolt.JsonParsingBolt;
import hcmus.group02.Spout.TwitterFileListeningSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


public class TwitterStormTopology
{
    private static final int ONE_MINUTES = 60000;

    private static void runLocalTopology(TopologyBuilder builder) throws Exception {
        String fields = "created_at,tweet_id,tweet,likes,retweet_count,source," +
                "user_id,user_name,user_screen_name,user_join_date,user_followers_count," +
                "city,country,state,state_code,collected_at";

        // Take input
        builder.setSpout("TwitterFileListeningSpout", new TwitterFileListeningSpout("data/hashtag_joebiden.json"));

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

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();
        try (LocalCluster cluster = new LocalCluster()) { // Try-with-resources
            cluster.submitTopology("LocalStormTopology", config, topology);
            Utils.sleep(ONE_MINUTES/6);
            cluster.killTopology("LocalStormTopology");
            cluster.shutdown();
        }
    }

    public static void main( String[] args ) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        runLocalTopology(builder);
    }
}
