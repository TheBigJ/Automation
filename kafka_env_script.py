# this script is responsible to purge the kafka topic data

import sys
from collections import defaultdict
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, ConfigResource, NewTopic
from kafka.errors import UnknownTopicOrPartitionError


def is_kafka_topics_purged(args, topics_per_tenant_to_execute, data):
    print("================Started kafka Topic data count.=======================")
    topicWithDataForAnyParition = set()
    flagDisplayKafkaResetTimeCommands = 1
    bootstrap_servers = data['brokers']
    brokers = " ".join(bootstrap_servers)
    GROUP = 'Planner'
    topicWithNoData = defaultdict(list)
    topicWithData = defaultdict(list)
    print("\nRunning for env = " + args.env)
    print("\nKafka brokers to be used = " + brokers + "\n")
    errorTopicList = []

    for topic in topics_per_tenant_to_execute:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=GROUP,
            enable_auto_commit=False
        )
        try:
            print(" checking for topic :=> " + topic)
            for p in consumer.partitions_for_topic(topic):
                tp = TopicPartition(topic, p)
                consumer.assign([tp])
                committed = consumer.committed(tp)
                consumer.seek_to_beginning(tp)
                first_offset = consumer.position(tp)
                consumer.seek_to_end(tp)
                last_offset = consumer.position(tp)
                count = last_offset - first_offset
                topicKeyWithparition = str(topic) + "_" + str(p)
                if count > 0:
                    topicWithData[topicKeyWithparition].append("Count=" + str(count))
                    topicWithDataForAnyParition.add(topic)

                elif count == 0:
                    topicWithNoData[topic].append(first_offset)
                    topicWithNoData[topic].append(last_offset)
                    topicWithNoData[topic].append(count)
            consumer.close(autocommit=False)
        except:
            print("Exception Occurred.", topic, "is not present or incorrect")
            errorTopicList.append(topic)
            break

    # printing topics with errors and exiting
    if len(errorTopicList) > 0:
        print("==========================================================================================")
        print("\nError: Below topics are failed; either topic not present or some issue with topic name : ")
        for k in errorTopicList:
            print("-> " + k)
        return False

    # printing topics with doesn't have any data
    if len(topicWithNoData) > 0:
        cnt = 1
        for k in topicWithNoData:
            if k not in topicWithDataForAnyParition:
                if cnt == 1:
                    print("\n===================Below topics are empty : =================================")
                    cnt = 2
                print(k)

    # preparing set 0 and set 7 day retentions command if topics having data
    zookeeperUrls = "<zookeeper_TO_BE_REPLACED_RUNTIME_AS_PERSETUP>"
    envlower = args.env.lower()
    resetZeroCommands = ""
    reset7daysCommands = ""
    if envlower == "dev" or envlower == "qa" or envlower == "perf":
        zookeeperUrls = "aosqa-zookeeper01.cloud.operative.com:2181,aosqa-zookeeper02.cloud.operative.com:2181,aosqa-zookeeper03.cloud.operative.com:2181"

    if envlower == "stg":
        zookeeperUrls = "awaos-stg-zookeeper01.cloud.operative.com:2181,awaos-stg-zookeeper02.cloud.operative.com:2181,awaos-stg-zookeeper03.cloud.operative.com:2181"

    if envlower == "prod":
        zookeeperUrls = "prod-zookeeper01.cloud.operative.com:2181,prod-zookeeper02.cloud.operative.com:2181,prod-zookeeper03.cloud.operative.com:2181"

    kafkaCommandForResetData = " kafka-topics --zookeeper " + zookeeperUrls + " --alter --topic $TOPICSNAME --config retention.ms=0"
    kafkaCommandSetRetensioPeriod = " kafka-topics --zookeeper " + zookeeperUrls + " --alter --topic $TOPICSNAME --config retention.ms=604800000"

    topics = ""
    for k in topicWithDataForAnyParition:
        if flagDisplayKafkaResetTimeCommands == 1:
            topics += k + ","

    topics = topics[:-1]
    finalCommandDataReset = kafkaCommandForResetData.replace("$TOPICSNAME", topics)
    finalCommand_7_day_retaintion = kafkaCommandSetRetensioPeriod.replace("$TOPICSNAME", topics)
    resetZeroCommands += finalCommandDataReset
    reset7daysCommands += finalCommand_7_day_retaintion
    # end preparing command for the retention period

    # print topics that does have data plus commands for retentions and exiting the process.
    if len(topicWithData) > 0:
        print("\n=========================Below Topics are having data in Kafka  : ================================")
        for k in topicWithData:
            print(k, topicWithData[k])
        print(
            "\n=========================Below command can be used to flush data from  topics.=========================")
        print(resetZeroCommands)
        print(
            "\n===================Below commands can be used to reset retention period as 7 days for topics which has cleaned up earlier=============================================")
        print(reset7daysCommands)
        return False

    elif len(topicWithData) == 0:
        print("Success: all topics are empty.")
        return True


def purge_topics_data(args, topics_per_tenant_to_execute, data):
    print("==============starting purging of data.============================")
    bootstrap_servers = data['brokers']
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='planner'
    )
    topic_list = []
    for topic in topics_per_tenant_to_execute:
        topic_list.append(
            ConfigResource(resource_type='TOPIC', name=topic, configs={"retention.ms": "0"}))
    admin_client.alter_configs(config_resources=topic_list)
    print("\ncompleted the purging of following topics: " + str(topics_per_tenant_to_execute))


def set_retention_period_7_days(args, topics_per_tenant_to_execute, data):
    print("==============starting setting retention_period of 7 days for topics.============================")
    bootstrap_servers = data['brokers']
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='planner'
    )
    topic_list = []
    for topic in topics_per_tenant_to_execute:
        topic_list.append(
            ConfigResource(resource_type='TOPIC', name=topic, configs={"retention.ms": "604800000"}))
    admin_client.alter_configs(config_resources=topic_list)
    print("\ncompleted retention_period of 7 days for topics: " + str(topics_per_tenant_to_execute))


def delete_topics(topic_names,data):
    print("==============starting deleting the topics.============================")
    bootstrap_servers = data['brokers']
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='planner'
    )
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError as e:
        print("Topic Doesn't Exist")
    except Exception as e:
        print(e)
    print("==============end deleted the topics topics.============================")


def create_topics(topic_names, data):
    print("==============starting  creation of topics.============================")
    bootstrap_servers = data['brokers']
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='planner'
    )
    topic_list = []
    for topic in topic_names:
        topic_list.append(NewTopic(name=topic, num_partitions=data["num_partitions"], replication_factor=data["replication_factor"]))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("\n=======================completed topics creation for: " + str(topic_names))


def describe_topics(topic_names, data):
    print("==============starting describing the topics topics.============================")
    bootstrap_servers = data['brokers']
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='planner'
    )
    print(admin_client.describe_topics(topic_names))
    print("\n=======================completed topics description for: " + str(topic_names))
