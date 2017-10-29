'''
Created on Oct 29, 2017

@author: bkehoe
'''

import time
import json
import base64

import boto3

from . import components, states, local

def create_components(boto3_session=None):
    boto3_session = boto3_session or boto3.Session()
    
    definition_store = S3DefinitionStore(boto3_session)
    
    execution_store = ClientContextAndDynamoDBExecutionStore()
    
    logger_factory = local.LocalLoggerFactory()
    
    
    task_dispatcher = LambdaTaskDispatcher(boto3_session)
    
    
    return {
        "definition_store": definition_store,
        "execution_store": execution_store,
        "logger_factory": logger_factory,
        "task_dispatcher": task_dispatcher,
    }

def create_and_configure_components(definition_bucket_name, boto3_session=None):
    comps = create_components(boto3_session)
    comps["definition_store"]._configure_bucket(definition_bucket_name)
    return comps

class S3DefinitionStore(components.DefinitionStore):
    _DEFINITION_CACHE = {}
    
    @classmethod
    def _check_definition_cache(cls, definition_id):
        return cls._DEFINITION_CACHE.get(definition_id)
    
    @classmethod
    def _cache_definition(cls, definition_id, definition):
        cls._DEFINITION_CACHE[definition_id] = definition
    
    DEFINITION_KEY_PREFIX = 'state_machine_definitions/'
    
    @classmethod
    def definition_key(cls, id):
        return '{}{}'.format(cls.DEFINITION_KEY_PREFIX, id)
    
    CONTEXT_DEFINITION_BUCKET_KEY = 'x-heaviside-sm-bucket'
    CONTEXT_DEFINITION_ID_KEY = 'x-heaviside-sm-def-id'
    
    def __init__(self, boto3_session=None):
        self.session = boto3_session or boto3.Session()
        
        self.bucket_name = None
        self.bucket = None
    
    def get_context(self):
        return {
            self.CONTEXT_DEFINITION_BUCKET_KEY: self.bucket_name
        }
    
    def hydrate(self, context):
        self._configure_bucket(context[self.CONTEXT_DEFINITION_BUCKET_KEY])
    
    def _configure_bucket(self, bucket_name):
        self.bucket_name = bucket_name
        self.bucket = self.session.resource('s3').Bucket(self.bucket_name)
    
    def put_anonymous(self, definition):
        print '[put anon]'
        definition_id = definition.get_hash()
        key = self.definition_key(definition_id)
        
        response = self.bucket.Object(key).put(
            Body=json.dumps(definition.to_json()),
            Metadata={self.CONTEXT_DEFINITION_ID_KEY: definition_id})
        
        print key, json.dumps(response)
        
        self._cache_definition(definition_id, definition)
        
        print self._DEFINITION_CACHE
        
        return definition_id
    
    def hydrate_definition(self, definition_id):
        print '[hydrate def] {}'.format(definition_id)
        definition = self._check_definition_cache(definition_id)
        if not definition:
            key = self.definition_key(definition_id)
            response = self.bucket.Object(key).get()
            definition = states.StateMachine.from_json(json.load(response['Body']))
            self._cache_definition(definition_id, definition)
        else:
            print 'cached'
        return definition

class ClientContextAndDynamoDBExecutionStore(components.ExecutionStore):
    def execution_factory(self, execution_id, definition_store):
        return self.Execution(execution_id, definition_store)
    
    class Execution(components.Execution):
        def __init__(self, execution_id, definition_store):
            components.Execution.__init__(self, execution_id, definition_store)
            
            self.current_state = None
            self.result = None
            self.definition_id = None
        
        def get_context(self):
            return {
                components.Execution.CONTEXT_CURRENT_STATE_KEY: self.current_state.to_json(),
                components.Execution.CONTEXT_DEFINITION_ID_KEY: self.definition_id,
            }
        
        def hydrate(self, context):
            self.current_state = components.State.from_json(context[self.CONTEXT_CURRENT_STATE_KEY])
            self.definition_id = context[self.CONTEXT_DEFINITION_ID_KEY]
        
        def initialize(self, definition):
            def_id = self.definition_store.put_anonymous(definition)
            
            self.definition_id = def_id
            
        def get_definition(self):
            return self.definition_store.hydrate_definition(self.definition_id)
        
        def change_state(self, new_state_name, data=None):
            self.current_state = components.State(new_state_name, data=data)
        
        def update_state_data(self, data):
            self.current_state.data = data
        
        def set_result(self, result):
            self.current_state = None
            self.result = result
        
        def get_current_state_and_result(self):
            return self.current_state, self.result

class CloudWatchLogger(components.ExecutorComponent):
    CONTEXT_LOG_SEQUENCE_TOKEN_KEY = 'x-heaviside-log-seq'
    
    def log_group_name(self):
        return 'heaviside'
    
    def log_stream_name(self):
        return '{}'.format(self.execution_id)
    
    def __init__(self, boto3_session, execution_id, executor_id):
        self.session = boto3_session
        
        self.execution_id = execution_id
        self.executor_id = executor_id
        
        self.logs = boto3.client('logs')
        self.sequence_token = None
    
    def get_context(self):
        return {
            self.CONTEXT_LOG_SEQUENCE_TOKEN_KEY: self.sequence_token,
        }
    
    def hydrate(self, context):
        self.sequence_token = context[self.CONTEXT_LOG_SEQUENCE_TOKEN_KEY]
    
    def log(self, message):
        kwargs = {
            "logGroupName": self.log_group_name(),
            "logStreamName": self.log_stream_name(),
            "logEvents": [
                {
                    'timestamp': long(time.time()),
                    'message': message,
                },
            ],
        }
        
        if self.sequence_token:
            kwargs["sequenceToken"] = self.sequence_token
        
        response = self.logs.put_log_events(**kwargs)
        
        self.sequence_token = response["sequenceToken"]
    
    def initialize(self):
        pass

class CloudWatchLoggerFactory(object):
    def __init__(self, boto3_session=None):
        self.session = boto3_session or boto3.Session()
    
    def logger_factory(self, execution_id, executor_id):
        return CloudWatchLogger()

class LambdaTaskDispatcher(components.TaskDispatcher):
    def __init__(self, boto3_session=None):
        self.session = boto3_session or boto3.Session()
        self.lambda_svc = self.session.client('lambda')
    
    def dispatch(self, resource, input, context):
        kwargs = {
            "FunctionName": resource,
            "Payload": json.dumps({'baz': 'bar'}),
            "InvocationType": "RequestResponse",
            "ClientContext": base64.b64encode(json.dumps({"custom": context})),
        }
        #kwargs["InvocationType"] = "DryRun"
        result = self.lambda_svc.invoke(**kwargs)
        print result['StatusCode'], result
        