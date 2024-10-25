package ru.romangontar;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import io.slurm.clients.userclick.UserClick;
import io.slurm.clients.aggregateduserclick.AggregatedUserClick;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args)
    {
        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregated_user_click_filter");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.81.253:19092,192.168.81.253:29092,192.168.81.253:39092");
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.81.253:8091");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://192.168.81.253:8091");
        final Serde<UserClick> userClickSerde = new SpecificAvroSerde<>();
        userClickSerde.configure(serdeConfig, false);
        final Serde<AggregatedUserClick> aggregatedUserClickSerde = new SpecificAvroSerde<>();
        aggregatedUserClickSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, UserClick> userClicks = builder.stream("user_clicks", Consumed.with(Serdes.String(), userClickSerde));
        KStream<Windowed<String>, AggregatedUserClick> aggregatedUserClicks = userClicks
                .groupBy((key, userClick) -> userClick.getWebsite().toString())
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)))
                .aggregate(AggregatedUserClick::new, (key, userClick, aggregatedUserClick) -> {
                    aggregatedUserClick.setWebsite(userClick.getWebsite());
                    aggregatedUserClick.setUsersClicked(aggregatedUserClick.getUsersClicked() + 1);
                    return aggregatedUserClick;
                }, Materialized.with(Serdes.String(), aggregatedUserClickSerde)).toStream();

        aggregatedUserClicks.to("aggregated_user_clicks");

        //System.out.println(builder.build().describe().toString());

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
