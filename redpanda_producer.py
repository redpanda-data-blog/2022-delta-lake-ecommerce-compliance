from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from time import sleep
from faker import Faker
import json
import os
from dotenv import load_dotenv


def main():
    topic_name = os.environ.get('REDPANDA_TOPIC')
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    modes_of_excercise = ['cycling', 'skiing', 'walking', 'jogging', 'running', 'swimming', 'climbing',
                          'football', 'basketball', 'tennis', 'badminton', 'volleyball', 'rugby', 'cricket', 'baseball']
    fake = Faker()

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=broker_ip)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    producer = KafkaProducer(bootstrap_servers=[broker_ip],
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    count = 0
    for i in range(100000):
        fake_date = fake.date_this_year(True, True)
        total_steps = fake.pyint(1500, 10000)
        exercise = fake.word(ext_word_list=modes_of_excercise)
        calories = fake.pyint(200, 875)
        active_minutes = fake.pyint(35, 100)
        sedantary_minutes = fake.pyint(5, 20)
        name = fake.name()
        str_date = str(fake_date)
        producer.send(topic_name, {'date': str_date, 'total_steps': total_steps, 'exercise': exercise,
                                   'calories_burnt': calories, 'active_mins': active_minutes,
                                   'sedantary_mins': sedantary_minutes})
        if count == 0:
            print("Inserted entry ", i, " to topic", topic_name)
        count = 1
        sleep(0.5)

load_dotenv()
main()
print('EXITED FROM MAIN LOOP')
