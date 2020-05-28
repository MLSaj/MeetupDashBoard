package MeetupRequests;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;


public class cloudMeetupCall {
    public final static String website = "http://stream.meetup.com/2/rsvps";

    public static String getHTML(String urlToRead) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;




        Properties prop = new Properties();
        prop.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.cloudBootstrapServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);



        int i = 0;
        while ((line = rd.readLine()) != null) {
            //JSONObject jsonObj = new JSONObject(line);

            producer.send(new ProducerRecord<String,String>(AppConfigs.meetUpTopic,Integer.toString(i),
                    line));
            System.out.println(line);
            i += 1;
            //result.append(line);

        }
        rd.close();
        return result.toString();
    }

    public static void main(String[] args) throws Exception {

        getHTML(website);
        //JSONObject json = new JSONObject
        //JSONObject jsonObj = new JSONObject("{\"phonetype\":\"N95\",\"cat\":\"WP\"}");
        //System.out.println(jsonObj);

    }
}
