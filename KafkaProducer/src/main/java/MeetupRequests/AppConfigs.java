package MeetupRequests;

public class AppConfigs {
    public final static String applicationID = "PosValidator";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String groupID = "PosValidatorGroup";
    public final static String[] sourceTopicNames = {"pos"};
    public final static String validTopicName = "valid-pos";
    public final static String invalidTopicName = "invalid-pos";

    public final static String cloudBootstrapServers = "10.150.0.3:9092,10.150.0.4:9092";

    public final static String meetUpTopic = "meetup";
}
