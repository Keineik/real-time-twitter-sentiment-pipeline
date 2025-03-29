package hcmus.group02;

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
        // Take input
        builder.setSpout("TwitterFileListeningSpout", new TwitterFileListeningSpout("data/hashtag_joebiden.json"));

        // Parse JSON
        builder.setBolt("JsonParsingBolt", new JsonParsingBolt("tweet_id,state,tweet"))
                .shuffleGrouping("TwitterFileListeningSpout");

        // Count tweet group by state
        builder.setBolt("StateCountingBolt", new StateCountingBolt())
                .fieldsGrouping("JsonParsingBolt", new Fields("state"));

        // Get sentiment of tweets
        builder.setBolt("SentimentBolt", new SentimentBolt("AFINN-en-165.txt"))
                .shuffleGrouping("JsonParsingBolt");

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();
        try (LocalCluster cluster = new LocalCluster()) { // Try-with-resources
            cluster.submitTopology("LocalStormTopology", config, topology);
            Utils.sleep(ONE_MINUTES/10);
            cluster.killTopology("LocalStormTopology");
            cluster.shutdown();
        }
    }

    public static void main( String[] args ) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        runLocalTopology(builder);
    }
}
