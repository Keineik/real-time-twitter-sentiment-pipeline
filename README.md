# Real-Time Twitter Sentiment Analysis Pipeline

A distributed real-time data processing pipeline built with Apache Storm for analyzing Twitter sentiment data. This project implements a streaming architecture that processes Twitter data, performs sentiment analysis, and stores results in PostgreSQL.

## 🏗️ Architecture

The pipeline consists of the following components:

- **Apache Storm**: Distributed real-time computation system
- **Apache Kafka**: Message streaming platform for data ingestion
- **PostgreSQL**: Database for storing processed results
- **Docker**: Containerized deployment environment

## 📋 Features

- **Real-time Processing**: Stream processing of Twitter data using Apache Storm
- **Sentiment Analysis**: AFINN-based sentiment scoring for tweets
- **Geographic Analysis**: Tweet counting and analysis by state
- **Windowed Analytics**: Time-based aggregations with configurable windows
- **Scalable Architecture**: Distributed processing with Storm topology
- **Database Persistence**: Automated data storage in PostgreSQL
- **Docker Support**: Easy deployment with Docker Compose

## 🚀 Getting Started

### Prerequisites

- Java 17+
- Maven 3.6+
- Docker & Docker Compose
- Apache Storm (if running locally)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Keineik/real-time-twitter-sentiment-pipeline.git
   cd real-time-twitter-sentiment-pipeline
   ```

2. **Build the project**
   ```bash
   mvn clean compile package
   ```

3. **Set up the infrastructure**
   ```bash
   make up
   ```

4. **Set up Kafka streams**
   ```bash
   make setup-stream
   ```

### Running the Pipeline

1. **Start the Storm topology**
   ```bash
   # For local development
   mvn exec:java -Dexec.mainClass="hcmus.group02.TwitterStormTopology"
   ```

2. **Produce test data to Kafka**
   ```bash
   make produce-stream
   ```

3. **Monitor the stream (optional)**
   ```bash
   make consume-stream
   ```

## 📂 Project Structure

```
├── src/
│   ├── main/
│   │   ├── java/hcmus/group02/
│   │   │   ├── TwitterStormTopology.java    # Main topology definition
│   │   │   ├── Bolt/
│   │   │   │   ├── JsonParsingBolt.java     # JSON parsing and validation
│   │   │   │   ├── SentimentBolt.java       # Sentiment analysis using AFINN
│   │   │   │   ├── StateCountingBolt.java   # Geographic tweet counting
│   │   │   │   ├── PostgresBolt.java        # Database persistence
│   │   │   │   └── TrendWindowingBolt.java  # Time-windowed analytics
│   │   │   └── Spout/
│   │   │       └── TwitterFileListeningSpout.java # File-based data source
│   │   └── resources/
│   │       ├── AFINN-en-165.txt            # Sentiment scoring dictionary
│   │       └── data/small_subset.json      # Sample Twitter data
│   └── test/
├── docker/
│   └── docker-compose.yaml                # Container orchestration
├── data/
│   ├── CSVToJson.py                       # Data conversion utility
│   └── README.txt                         # Data setup instructions
├── Makefile                               # Build and deployment commands
└── pom.xml                               # Maven dependencies
```

## 🔧 Configuration

### Storm Topology Configuration

The main topology (`TwitterStormTopology.java`) defines the data flow:

1. **Data Ingestion**: 
   - `TwitterFileListeningSpout`: Reads from JSON files
   - `KafkaSpout`: Consumes from Kafka topics

2. **Processing Bolts**:
   - `JsonParsingBolt`: Parses and validates JSON tweets
   - `SentimentBolt`: Calculates sentiment scores using AFINN dictionary
   - `StateCountingBolt`: Aggregates tweets by geographic state
   - `TrendWindowingBolt`: Performs time-based windowed analytics
   - `PostgresBolt`: Persists processed data to database

### Tweet Data Schema

The pipeline processes tweets with the following fields:
```
created_at, tweet_id, tweet, likes, retweet_count, source,
user_id, user_name, user_screen_name, user_join_date, 
user_followers_count, city, country, state, state_code, collected_at
```

## 🐳 Docker Services

The `docker-compose.yaml` includes:

- **Zookeeper**: Coordination service for Storm and Kafka
- **Storm Nimbus**: Master node for topology management
- **Storm Supervisor**: Worker nodes for task execution
- **Storm UI**: Web interface for monitoring (typically port 8080)
- **Kafka**: Message streaming platform
- **PostgreSQL**: Database for data persistence

## 📊 Monitoring

- **Storm UI**: Access the Storm web interface to monitor topology performance
- **Kafka**: Use Kafka console tools for stream monitoring
- **Database**: Query PostgreSQL for processed results

## 🛠️ Development

### Adding New Bolts

1. Create a new class extending `BaseRichBolt` in the `Bolt/` package
2. Implement required methods: `prepare()`, `execute()`, `declareOutputFields()`
3. Add the bolt to the topology in `TwitterStormTopology.java`

### Testing

Run unit tests with:
```bash
mvn test
```

## 📈 Performance Tuning

- Adjust parallelism hints in topology configuration
- Configure Storm worker memory and CPU allocation
- Optimize batch sizes for database operations
- Tune Kafka consumer settings for throughput

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request

## 📄 License

This project is part of an academic assignment at HCMUS (Ho Chi Minh City University of Science).

## 🏫 Team

**Group 02 - HCMUS**

## 🔗 Dependencies

- Apache Storm 2.8.0
- Apache Kafka 3.9.0
- PostgreSQL JDBC Driver 42.7.5
- JUnit 3.8.1 (testing)

## 📚 Additional Resources

- [Apache Storm Documentation](https://storm.apache.org/releases/current/index.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [AFINN Sentiment Lexicon](https://github.com/fnielsen/afinn)
