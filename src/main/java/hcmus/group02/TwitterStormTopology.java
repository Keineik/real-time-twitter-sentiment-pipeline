package hcmus.group02;

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
        builder.setSpout("TwitterFileListeningSpout", new TwitterFileListeningSpout("data/hashtag_joebiden.json"));

        builder.setBolt("TweetExtractingBolt", new JsonParsingBolt("state"))
                .shuffleGrouping("TwitterFileListeningSpout");

        builder.setBolt("StateCountingBolt", new StateCountingBolt())
                .fieldsGrouping("TweetExtractingBolt", new Fields("state"));

        Config config = new Config();
        config.setDebug(true);


        StormTopology topology = builder.createTopology();
        try (LocalCluster cluster = new LocalCluster()) { // Try-with-resources
            cluster.submitTopology("github-commit-count-topology", config, topology);
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
