from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from time import sleep
from faker import Faker
import faker_commerce
from faker.providers import internet
import json
import os
from dotenv import load_dotenv


def main():
    topic_name = os.environ.get('REDPANDA_TOPIC')
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    types_of_categories = ['pants', 'shirts', 't-shirts', 'ties', 'jewellery', 'gaming', 'electronics',
                             'sports', 'kitchenwear', 'wardrobe', 'baking', 'stationary', 'utensile', 'gadgets',
                             'plastic', 'gardening', 'food', 'beverages', 'lounge']
    fake = Faker()
    fake.add_provider(faker_commerce.Provider)
    fake.add_provider(internet)
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=broker_ip)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    producer = KafkaProducer(bootstrap_servers=[broker_ip],
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    url = 'https://sample.ecomm.com/product/'
    for i in range(100000):
        fake_date = fake.date_this_year(True, True)
        product_id = fake.pyint(1,100000)
        url = url + '/' + str(product_id)
        categories = fake.word(ext_word_list=types_of_categories)
        product_name = fake.ecommerce_name()
        name = fake.name()
        email_addr = fake.email(name)
        str_date = str(fake_date)
        login_duration = fake.pyint(1500, 10000)
        ipv4_addr = fake.ipv4_public()
        items_in_cart = fake.pyint(1,25)
        price_in_cart = fake.pyint(20,1000)
        country = fake.country()
        producer.send(topic_name, {'date': str_date, 'product_name': product_name, 'category': categories,
                                   'name': name, 'email': email_addr, 'ip_addr': str(ipv4_addr),
                                   'items_in_cart': items_in_cart, 'price_in_cart': price_in_cart, 'country': country, 'url': url,
                                   'login_duration': login_duration})
        print("Inserted entry ", i, " to topic", topic_name)
        sleep(5)

load_dotenv()
main()
print('EXITED FROM MAIN LOOP')
