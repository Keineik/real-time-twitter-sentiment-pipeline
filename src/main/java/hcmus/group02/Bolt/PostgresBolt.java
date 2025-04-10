package hcmus.group02.Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PostgresBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresBolt.class);
    private OutputCollector collector;
    Connection connection;
    private final List<String> fields;
    private final String type;

    public PostgresBolt(String fields, String type) {
        this.fields = Arrays.asList(fields.split(","));
        this.type = type;
    }

    public void prepare(Map<String, Object> map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            // Establish connection
            String dbUrl = "jdbc:postgresql://localhost:5432/postgres";
            String user = "admin";
            String password = "admin";
            this.connection = DriverManager.getConnection(dbUrl, user, password);

            // Create table if not exists
            try (Statement statement = connection.createStatement()) {
                String sql = """
                        CREATE TABLE IF NOT EXISTS Tweet (
                             id SERIAL PRIMARY KEY,
                             created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                             tweet_id VARCHAR(30) UNIQUE NOT NULL,
                             tweet TEXT NOT NULL,
                             likes INT DEFAULT 0,
                             retweet_count INT DEFAULT 0,
                             source VARCHAR(255),
                             user_id VARCHAR(30) NOT NULL,
                             user_name VARCHAR(255),
                             user_screen_name VARCHAR(255),
                             user_join_date TIMESTAMP WITH TIME ZONE,
                             user_followers_count INT DEFAULT 0,
                             city VARCHAR(255),
                             country VARCHAR(255),
                             state VARCHAR(255),
                             state_code VARCHAR(10),
                             collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                             sentiment_score INT DEFAULT 0
                         );
                        
                        CREATE TABLE IF NOT EXISTS TweetCountByState (
                            id SERIAL PRIMARY KEY,
                            state VARCHAR(255) UNIQUE NOT NULL,
                            tweet_cnt INT DEFAULT 0
                        );
                        
                        CREATE TABLE IF NOT EXISTS TrendingTweets (
                            id SERIAL PRIMARY KEY,
                            hashtag VARCHAR(127),
                            tweet_cnt INT DEFAULT 0,
                            rank INT DEFAULT 0,
                            window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                            window_end TIMESTAMP WITH TIME ZONE NOT NULL
                        )""";
                statement.executeUpdate(sql);
                System.out.println("Table created/verified successfully");

                if (Objects.equals(type, "new table")) {
                    // Delete old data if exists since this is in development
                    sql = "DELETE FROM Tweet WHERE 1=1";
                    statement.executeUpdate(sql);
                    sql = "DELETE FROM TweetCountByState WHERE 1=1";
                    statement.executeUpdate(sql);
                    sql = "DELETE FROM TrendingTweets WHERE 1=1";
                    statement.executeUpdate(sql);
                }
                System.out.println("Table cleared successfully");
            }
        } catch (SQLException e) {
            if (e.getSQLState().equals("23505")) {
                LOGGER.warn("Duplicate sequence detected, ignoring: {}", e.getMessage());
            }
            else throw new RuntimeException("Table creation failed" + e.getMessage());
        }
    }

    public void execute(Tuple tuple) {
        String source = tuple.getSourceComponent();
        if (source.equals("JsonParsingBolt")) {
            String sql = """
                    INSERT INTO Tweet (created_at, tweet_id, tweet, likes, retweet_count, source,
                                       user_id, user_name, user_screen_name, user_join_date, user_followers_count,
                                       city, country, state, state_code, collected_at)
                    VALUES (?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?);""";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int idx = 1;
                statement.setTimestamp(idx++, Timestamp.from(OffsetDateTime.parse(tuple.getStringByField("created_at")).toInstant()));
                statement.setString(idx++, tuple.getStringByField("tweet_id"));
                statement.setString(idx++, tuple.getStringByField("tweet"));
                statement.setInt(idx++, (int) Double.parseDouble(tuple.getStringByField("likes")));
                statement.setInt(idx++, (int) Double.parseDouble(tuple.getStringByField("retweet_count")));
                statement.setString(idx++, tuple.getStringByField("source"));
                statement.setString(idx++, tuple.getStringByField("user_id"));
                statement.setString(idx++, tuple.getStringByField("user_name"));
                statement.setString(idx++, tuple.getStringByField("user_screen_name"));
                statement.setTimestamp(idx++, Timestamp.from(OffsetDateTime.parse(tuple.getStringByField("user_join_date")).toInstant()));
                statement.setInt(idx++, (int) Double.parseDouble(tuple.getStringByField("user_followers_count")));
                statement.setString(idx++, tuple.getStringByField("city"));
                statement.setString(idx++, tuple.getStringByField("country"));
                statement.setString(idx++, tuple.getStringByField("state"));
                statement.setString(idx++, tuple.getStringByField("state_code"));
                statement.setTimestamp(idx, Timestamp.from(OffsetDateTime.parse(tuple.getStringByField("collected_at")).toInstant()));

                statement.executeUpdate();
                collector.emit(new Values(tuple.getValues().toArray(new Object[0])));
                collector.ack(tuple);
            } catch (Exception e) {
                LOGGER.error("Error persisting from JsonParsingBolt: {}", e.getMessage());
                collector.fail(tuple);
            }
        }
        else if (source.equals("SentimentBolt")) {
            String sql = """
                    UPDATE Tweet SET sentiment_score = ? WHERE tweet_id = ?""";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, tuple.getIntegerByField("sentiment_score"));
                statement.setString(2, tuple.getStringByField("tweet_id"));

                int affectedRows = statement.executeUpdate();
                // If update failed
                if (affectedRows == 0) {
                    throw new SQLException("Failed to persist sentiment: " + tuple.getStringByField("tweet_id"));
                }
            }
            catch (Exception e) {
                LOGGER.error("Error persisting from SentimentBolt: {}", e.getMessage());
                collector.fail(tuple);
            }
        }
        else if (source.equals("StateCountingBolt")) {
            String sql = """
                    INSERT INTO TweetCountByState (state, tweet_cnt)
                    VALUES (?, ?)
                    ON CONFLICT (state) DO UPDATE SET tweet_cnt = EXCLUDED.tweet_cnt;""";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                var val = tuple.getValueByField("state");
                statement.setString(1, (val != null) ? val.toString() : "unknown");
                statement.setInt(2, tuple.getIntegerByField("tweet_cnt"));
                statement.executeUpdate();
            }
            catch (Exception e) {
                LOGGER.error("Error persisting from StateCountingBolt: {}", e.getMessage());
                collector.fail(tuple);
            }
        }
        else if (source.equals("TrendWindowingBolt")) {
            String sql = """
            INSERT INTO TrendingTweets (hashtag, tweet_cnt, rank, window_start, window_end)
            VALUES (?, ?, ?, ?, ?);""";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, tuple.getStringByField("hashtag"));
                statement.setInt(2, tuple.getIntegerByField("tweet_cnt"));
                statement.setInt(3, tuple.getIntegerByField("rank"));

                // Convert timestamps
                Timestamp windowStart = (Timestamp) tuple.getValueByField("window_start");
                Timestamp windowEnd = (Timestamp) tuple.getValueByField("window_end");

                statement.setTimestamp(4, windowStart);
                statement.setTimestamp(5, windowEnd);

                statement.executeUpdate();
                collector.ack(tuple);
            }
            catch (Exception e) {
                LOGGER.error("Error persisting from TrendWindowingBolt: {}", e.getMessage());
                collector.fail(tuple);
            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }
}
