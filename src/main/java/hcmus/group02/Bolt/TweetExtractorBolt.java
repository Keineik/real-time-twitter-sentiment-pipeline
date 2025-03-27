package hcmus.group02.Bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TweetExtractorBolt extends BaseBasicBolt {
    @Override
    // Indicates the bolt emits a tuple with needed fields
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "created_at",
                "tweet_id",
                "tweet",
                "likes",
                "retweet_count",
                "user_id",
                "user_name",
                "user_screen_name",
                "user_description",
                "user_join_date",
                "user_followers_count",
                "user_location",
                "lat",
                "long",
                "city",
                "country",
                "continent",
                "state",
                "state_code",
                "collected_at"));
    }

    // Gets called when a tuple has been emitted to this bolt
    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        String line = tuple.getStringByField("line");
        String[] parts = line.split(",");
        outputCollector.emit(new Values((Object[]) parts));
    }
}
