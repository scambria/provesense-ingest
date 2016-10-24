from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import randint
import time 

try:
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

    #minimum observation data
    graph = """
        @prefix ssn: <http://purl.oclc.org/NET/ssnx/ssn#> . 
        @prefix prov: <http://www.w3.org/ns/prov#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix : <http://provo.ssn.org/provesense#> .
        
        {} a ssn:SensorOutput ; 
            ssn:hasValue {}  .
        
        {} a ssn:Observation ;
            ssn:observedBy  {} ;
            ssn:observationResult {} ;
            prov:wasAttributedTo {} .                        
        """

    gen_tasks = {}
    gen_tasks['0'] = []
    new_task_c = 0
    task = 0
    sensor_c = 0        
    activity_c = 0
    user_c = 0
    user = 0
    activity = 0
    task_ceiling = 50
    
    # produce asynchronously
    for i in range(999999999):

        id = str(i)
        task = randint(0, len(gen_tasks) - 1)
        gen_tasks[str(task)] = [] 
        graph_updated = graph

        if sensor_c == 0: 
            sensor = randint(1, 5000)
            sensor_c = sensor
            data = """
        {} a ssn:Sensor ;
            ssn:observes  "twitter" ;
            prov:actedOnBehalfOf {} .        
            """
            data = data.format(':traptor-'+str(sensor),':twitter-task-'+str(task))
            graph_updated += data
            
        sensor_c-=1 
                
        graph_updated = graph_updated.format(":traptor-output-" + id, "\"observationValue" + id + "\"" , ":twitter-observation-" + id, 
                                             ":traptor-" + str(sensor), ":traptor-output-" + id, ":twitter-task-" + str(task))

        if i == 0:
            data = """
        :software-agent-1 a prov:SoftwareAgent, prov:entityInfluence ;
           prov:actedOnBehalfOf :activity-6476 .
           
        :shooting a prov:InstantaneousEvent ;
           prov:atTime "2016-06-12T02:02:00Z"^^xsd:dateTime .            
        
        {} a :UserTaskActivate ;
        
        {} a prov:Entity ;
           prov:wasGeneratedBy {} ;
           prov:wasAttributedTo {} ;
           prov:value "shooting" ;
           :task-type "keyword" .           
        """
            graph_updated += data.format(':user-task-activate-'+str(task), ':twitter-task-'+str(task), ':user-task-activate-'+str(task), ':user-'+str(user))

        graph_updated = graph_updated.replace(":traptor-001", ":traptor-" + str(sensor))

        if user_c == 0:
            user = randint(1, 10000)
            user_c = user
            data = """
        {} a prov:Person, prov:entityInfluence ;
           prov:actedOnBehalfOf {} .
        """
            data = data.format(':user-'+str(user), ':activity-'+str(activity))
            graph_updated += data
        user_c-=1

        if activity_c == 0:
            activity = randint(1, 10000)
            activity_c = activity
            graph_updated += """
        :activity-{} a prov:Activity . 
            """
            graph_updated = graph_updated.format(str(activity))
            
        activity_c-=1
        graph_updated = graph_updated.replace(":activity-001", ":activity-" + str(activity))        

        #generate random data quantities/ids 
        if new_task_c == 0:
            new_task = randint(1, task_ceiling)
            while str(new_task) in gen_tasks:
                if len(gen_tasks) == task_ceiling+1: 
                    task_ceiling += 100
                    new_task = task_ceiling
                else:
                    new_task = randint(1, task_ceiling)
            new_task_c = new_task
            gen_tasks[str(new_task)] = []
            #create collection if one does not already exist
            if not gen_tasks[str(task)]:                
                data = """                     
        {} a :AutoTaskActivate ; 
           prov:startedAtTime "2016-06-12T03:30:00Z"^^xsd:dateTime ;
           prov:used {} ;
           prov:wasAssociatedWith {} ;
           prov:generated {} .
                
        {} a prov:Collection, prov:Generation ;
           prov:wasGeneratedBy {} ;
           prov:qualifiedGeneration [
              a prov:Generation, prov:InstantaneousEvent ;
              prov:atTime "2016-06-12T02:02:00Z"^^xsd:dateTime ;
              prov:activity :shooting ;
           ] . 
                """
                
                data = data.format(':activate-twitter-task-' + str(task) + '-descendants',
                                   ':twitter-task-' + str(task),
                                   ':software-agent-1',
                                   ':twitter-task-' + str(task) + '-descendants',
                                   ':twitter-task-' + str(task) + '-descendants',
                                   ':activate-twitter-task-' + str(task) + '-descendants')                
                graph_updated += data

            #add new task                
            gen_tasks[str(task)].append(str(new_task))            
           
            data = """
        {} a prov:Entity ;
           prov:wasGeneratedBy  {} ;
           #hadPrimarySource subproperty of wasDerivedFrom (swapped for influencedBy)
           prov:wasInfluencedBy  {} ;
           #wasAttributedTo subproperty of wasInfluencedBy
           prov:wasAttributedTo {} .
            """
            data = data.format(":twitter-task-" + str(new_task), 
                               ":activate-twitter-task-" + str(new_task),
                               ":twitter-task-" + str(task), 
                               ":software-agent-1")
            graph_updated += data           
        new_task_c-=1        

        producer.send('provesense.inbound', bytes(graph_updated.encode('UTF-8')))
        
        time.sleep(.005)

    # block until all async messages are sent
    producer.flush()
    
    
except Exception as e:
    # Decide what to do if produce request failed...
    print(e)


# configure multiple retries
#producer = KafkaProducer(retries=5)