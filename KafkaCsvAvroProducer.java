///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.6.3
///usr/bin/env jbang "$0" "$@" ; exit $?
//REPOS confluent=https://packages.confluent.io/maven/
//REPOS mavencentral,acme=https://maven.acme.local/maven

//DEPS info.picocli:picocli:4.6.3
//DEPS org.apache.kafka:kafka-clients:3.3.1
//DEPS org.slf4j:slf4j-log4j12:2.0.5
//DEPS io.confluent:confluent-log4j:1.2.17-cp10
//DEPS io.confluent:kafka-avro-serializer:7.3.0
//DEPS com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.13.0



import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


import java.util.concurrent.Callable;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;


import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

@Command(name = "KafkaCsvAvroProducer", mixinStandardHelpOptions = true, 
        version = "KafkaCsvAvroProducer 0.1",
        description = "KafkaCsvAvroProducer to publish Avro Data in Kafka topic")
class KafkaCsvAvroProducer implements Callable<Integer> {

    @Option(required = true, 
    description = "Topic name", help = true, names = {"-t", "--topic"},
    defaultValue="test-topic")
    private String topicStr;

    @Option(required = false, 
    description = "Producer Configfile", help = true, names = {"-p", "--producer.config"})
    private String producerConfigFileStr;

    @Option(required = false, description = "Schema String", 
            help = true, names = {"-s", "--schema-string"},
            defaultValue = "{\"type\":\"record\"," +
            "\"name\":\"myrecord\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":[\"null\", \"string\"]}]}")
    private String schemaString;

    @Option(required = false, description = "csv file path", 
    help = true, names = {"-c", "--csv-file"},
    defaultValue = "/tmp/file.csv")
    private String csvFileStr;


    private Schema schema; 


    public static void main(String... args) {
        int exitCode = new CommandLine(new KafkaCsvAvroProducer()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception { // your business logic goes here...
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
            "http://localhost:8081");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);


        System.out.printf("building the schema:%s \n", schemaString);    
        schema = new Schema.Parser().parse(schemaString);  

        Producer<String, GenericRecord> producer = 
            new KafkaProducer<String, GenericRecord>(props);

        File csvFile = new File(csvFileStr);
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader(); // use first row as header; otherwise defaults are fine
        MappingIterator<Map<String,String>> it = mapper.readerFor(Map.class)
            .with(schema)
            .readValues(csvFile);
        while (it.hasNext()) {
            Map<String,String> rowAsMap = it.next();
            // access by column name, as defined in the header row...
            GenericRecord record = buildRecord(rowAsMap);
            producer.send(new ProducerRecord<String,GenericRecord>(topicStr, 
                        ""+System.currentTimeMillis(), 
                        record), 
                        (recordMetadata, ex) -> {
                            if (ex != null) {
                                System.err.println(ex);
                                return;
                            }
                            System.out.printf("Partition %d, Offset - %d", 
                                recordMetadata.partition(), 
                                recordMetadata.offset());
   
                        });
        }
        
        producer.flush();
        producer.close();


        return 0;
    }

    /**
     * @return
     * @throws FileNotFoundException
     */
    private final GenericRecord buildRecord(Map<String, String> rowAsMap) throws FileNotFoundException {
        GenericData.Record record = new GenericData.Record(schema);
        for (Entry<String, String> entry : rowAsMap.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }      
        return record;
    }
}
