RabbitMQ sharding

Objective: 
    Implement RabbitMQ sharding, so that we can maintain order of published events of the same entity.

Example:
    I have a queue named image_queue. I have attached 2 consumers with that queue. Now I publish events in the following order in the image_queue queue.
    
        Image1.created 
        Image2.created 
        Image1.updated
        Image2.updated 
        Image1.updated
        Image1.deleted
        Image2.deleted.
        
    Then all the events of the Image1 should be consumed by Consumer-1 and all the event of Image2 should be consumed by Consumer-2 like following
    
    Consumer-1: 
        Image1.created
        Image1.updated
        Image1.updated
        Image1.deleted
        
    Consumer-2: 
        Image2.created
        Image2.updated
        Image2.deleted

Note: Here We have used 2 types of Images like Image-1 and Image-2 in above example. But it is possible that we can have many types of Images like 1000 types. So In that case also the events should be divided between 2 consumers that the same type of event will be consumed by the same consumer.

Advantages: 
    Here, the same type of event will be consumed by the same consumer, So it will consume in the same order which they were published.

Disadvantages
    1. There's a small race condition between RabbitMQ updating the queue's internal stats about consumers and when clients issue basic.consume commands. The problem with this is that if your client issues many basic.consume commands without too much time in between, it might happen that the plugin assigns the consumers to queues in an uneven way. Like one queue is assigned 2 consumers and another queue is assigned no consumers.(This problem can be solved by putting some delay on each basic.consume)
    
    2. The number of consumers and the number of logical queues should be the same. Suppose I have configured 3 logical queues and for that I have 3 consumers. If we want to add a new pod of that service, then the number of consumers becomes 6, So we need to increase the number of logical queues to 6.
    
    3. Suppose we have 2 pods running of any service, If there are 3 consumers in each pod, then there are 6 consumers running. If we kill one pod then there will be only 3 consumers but the number of logical queues are still 6. In this case we need to manually delete that 3 unused queues otherwise the messages will be published in that 3 unused queues and there will be no consumers of that.
    
    4. The routing_key must be random, otherwise messages will be published to only one logical queue and other queues will be idle.
    
    
How to use the POC:

Step: 1: Invoke the file declare_exchange.py

Step: 2: Invoke following command fron the terminal for windows os.For other os please get reference from https://www.rabbitmq.com/parameters.html
        command: rabbitmqctl.bat set_policy sharding_test-shard ^
                "^sharding_test$" "{""shards-per-node"":2, ""routing-key"":""sharding_test""}" ^
                --apply-to exchanges
                
 Step: 3: Invoke the file consume_rabbitmq_sharding_example.py
 
 Step: 4: Invoke the file publish_rabbitmq_sharding_example.py
