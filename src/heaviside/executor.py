'''
Created on Oct 28, 2017

@author: bkehoe
'''

from __future__ import absolute_import

import json
import base64
import uuid
import hashlib

import boto3

from . import states

_DEFINITION_CACHE = {}

def _get_definition(definition_hash):
    global _DEFINITION_CACHE
    return _DEFINITION_CACHE.get(definition_hash)

def _cache_definition(definition_hash, definition):
    global _DEFINITION_CACHE
    _DEFINITION_CACHE[definition_hash] = definition

def is_heaviside_execution(event, context):
    """Check if this Lambda invocation is associated with a state machine"""
    return StateMachine._ID_KEY in (context.client_context or {})

class StateMachine(object):
    """Class to manage state machine execution."""
    @classmethod
    def _state_machine_key(cls, id):
        return 'state_machines/{}'.format(id)
    
    @classmethod
    def create(cls, definition, state_machine_bucket_name, state_table_name, boto3_session=None, local=False):
        """Instantiate a new execution of the given state machine definition.
        Does not begin execution, that starts with the first dispatch() call.
        If not local, writes definition to S3""" 
        id = uuid.uuid4().hex
        
        definition = states.StateMachine.from_json(definition)
        definition_str = json.dumps(definition.to_json())
        definition_hash = hashlib.sha256().update(definition_str).hexdigest()
        
        if not local:
            boto3_session = boto3_session or boto3.Session()
            
            state_machine_bucket = boto3_session.resource('s3').Bucket(state_machine_bucket_name)
            
            response = state_machine_bucket.Object(cls._state_machine_key(id)).put(
                Metadata={cls._DEFINITION_HASH_KEY: definition_hash})
        else:
            _cache_definition(definition_hash, definition)
        
        return cls(id, definition, definition_hash, None, boto3_session, state_machine_bucket_name, state_table_name)
    
    @classmethod
    def _hydrate(cls, id, definition_hash, current_state, state_machine_bucket_name, state_table_name, boto3_session):
        definition = _get_definition(definition_hash) #try to find the definition in the cache, so we don't have to hit S3 
        if not definition:
            boto3_session = boto3_session or boto3.Session()
            state_machine_bucket = boto3_session.resource('s3').Bucket(state_machine_bucket_name)
            response = state_machine_bucket.Object(cls._state_machine_key(id)).get()
            definition = states.StateMachine.from_json(json.load(response['Body']))
            definition_hash = response['Metadata'][cls._DEFINITION_HASH_KEY]
            _cache_definition(definition_hash, definition)
        
        cls(id, definition, definition_hash, current_state, boto3_session, state_machine_bucket_name, state_table_name)
    
    _ID_KEY = 'x-heaviside-sm-id'
    _DEFINITION_HASH_KEY = 'x-heaviside-sm-def-hash'
    _STATE_MACHINE_BUCKET_KEY = 'x-heaviside-sm-bucket' 
    _STATE_TABLE_KEY = 'x-heaviside-sm-table'
    _STATE_KEY = 'x-heaviside-sm-state'
    
    @classmethod
    def hydrate(cls, context, boto3_session=None, local=False):
        """Retrieve the existing execution using the Lambda context.
        If the definition is in the cache, it won't need to go to S3."""
        client_context = context.client_context
        
        id = client_context[cls._ID_KEY]
        state_machine_bucket_name = client_context[cls._STATE_MACHINE_BUCKET_KEY]
        state_table_name = client_context[cls._STATE_TABLE_KEY]
        definition_hash = client_context[cls._DEFINITION_HASH_KEY]
        current_state = client_context[cls._STATE_KEY]
        
        return cls._hydrate(id, definition_hash, current_state, state_machine_bucket_name, state_table_name, boto3_session)
        
    def __init__(self, id, definition, definition_hash, current_state, boto3_session, state_machine_bucket_name, state_table_name):
        self.id = id
        self.definition = definition
        self._definition_hash = definition_hash
        
        self.current_state_name = current_state['name']
        self.current_state_data = current_state['data'] # anything the state needs to keep around
        self.result = None
        
        self.boto3_session = boto3_session
        self.state_machine_bucket_name = state_machine_bucket_name
        self.state_table_name = state_table_name
    
    def _record_state(self):
        print json.dumps(self._get_state(), indent=2)
        pass
    
    def _get_state(self):
        return {
            'name': self.current_state_name,
            'data': self.current_state_data,
        }
    
    def dispatch(self, input):
        """Run the state machine up to the next Task state, which will be async invoked."""
        if self.current_state_name is None:
            if self.result:
                return
            else:
                self.current_state_name = self.definition.start_at
        while True:
            state = self.definition.states[self.current_state_name]
            if isinstance(state, states.SuccessState):
                self.result = {
                    "type": "Success",
                    "name": self.current_state_name,
                }
                self.current_state_name = None
                self._record_state()
                break
            elif isinstance(state, states.FailState):
                self.result = {
                    "type": "Fail",
                    "name": self.current_state_name,
                    "Error": state.error,
                    "Cause": state.cause,
                }
                self.current_state_name = None
                self._record_state()
                break
            elif isinstance(state, states.TaskState):
                resource = state.resource
                lambda_svc = self.boto3_session.client('lambda')
                client_context = {
                    self._ID_KEY: self.id,
                    self._STATE_MACHINE_BUCKET_KEY: self.state_machine_bucket_name,
                    self._STATE_TABLE_KEY: self.state_table_name,
                    self._STATE_KEY: self._get_state(),
                    self._DEFINITION_HASH_KEY: self._definition_hash,
                }
                kwargs = {
                    "FunctionName": resource,
                    "Payload": json.dumps(input),
                    "InvocationType": "Event",
                    "ClientContext": base64.b64encode(json.dumps(client_context)),
                }
                self._record_state()
                result = lambda_svc.invoke(**kwargs)
                break
            else:
                raise TypeError("No matching type for {}".format(state))
    
    def run_task(self, task_function, exception_handler):
        """Process the current task and dispatch.
        Assumes the current state is a Task state."""
        state = self.definition.states[self.current_state_name]
        try:
            result = task_function()
        except Exception as e:
            exception = exception_handler(e)
            for catcher in state.catch or []:
                if catcher.matches(exception):
                    self.current_state_name = catcher.next
                    result = {}
                    break
            else:
                self.result = {
                    "type": "Fail",
                    "name": self.current_state_name,
                    "Error": "States.TaskFailed",
                    "Cause": "No matching catcher",
                }
                self.current_state_name = None
                self._record_state()
                return
        else:
            self.current_state_name = state.next
        self.dispatch(result)
        return result