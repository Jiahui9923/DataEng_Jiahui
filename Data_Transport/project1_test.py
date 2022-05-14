    
    
    
def acked(err, msg):
    global delivered_records

    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    with open("new_data.json",'r') as f:
        temp = json.loads(f.read())
        for x in temp:
            for key in x:
                record_key = "test1"
                record_value = (key + ': ' + x[key])
                print("Producing record: {}\t{}".format(record_key, record_value))
                producer.produce(topic, key = record_key, value = record_value, on_delivery = acked)
                #p.poll() serves delivery reports (on_delivery)
                #from previous produce() calls.
                producer.pool(1)
    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))