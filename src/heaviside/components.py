'''
Created on Oct 29, 2017

@author: bkehoe
'''

from __future__ import absolute_import

class State(object):
    @classmethod
    def from_json(cls, obj):
        return cls(obj['Name'], data=obj.get('Data'))
    
    def __init__(self, name, data=None):
        self.name = name
        self.data = data
    
    def to_json(self):
        obj = {
            'Name': self.name
        }
        if self.data is not None:
            obj['Data'] = self.data
        return obj

class Result(object):
    STATUS_SUCCEEDED = "SUCCEEDED"
    STATUS_FAILED = "FAILED"
    STATUS_TIMED_OUT = "TIMED_OUT"
    STATUS_ABORTED = "ABORTED"
    
    @classmethod
    def from_json(cls, obj):
        return cls(obj["Status"], obj.get("Output"))
    
    def __init__(self, status, output=None):
        self.status = status
        self.output = output
    
    def to_json(self):
        obj = {
            "Status": self.status
        }
        if self.output is not None:
            obj["Output"] = self.output
        return obj

class ExecutorComponent(object):
    def get_context(self):
        raise NotImplementedError
    
    def hydrate(self, context):
        raise NotImplementedError

class DefinitionStore(ExecutorComponent):
    def put_anonymous(self, anonymous_id, definition):
        raise NotImplementedError
    
    def hydrate_definition(self, definition_id):
        raise NotImplementedError

class Execution(ExecutorComponent):
    CONTEXT_CURRENT_STATE_KEY = 'x-heaviside-sm-cstate'
    CONTEXT_DEFINITION_ID_KEY = 'x-heaviside-sm-def'
    
    def __init__(self, execution_id, definition_store):
        self.execution_id = execution_id
        self.definition_store = definition_store
    
    def initialize(self, definition, execution_store):
        raise NotImplementedError
    
    def get_definition(self):
        raise NotImplementedError
    
    def change_state(self, new_state_name):
        raise NotImplementedError
    
    def update_state_data(self, data):
        raise NotImplementedError
    
    def get_current_state_and_result(self):
        raise NotImplementedError
    
    def set_result(self, result):
        raise NotImplementedError

class ExecutionStore(object):
    def execution_factory(self, execution_id, definition_store):
        raise NotImplementedError

class Logger(ExecutorComponent):
    def format(self, execution_id, executor_id, resource, state_name, message):
        return '[{}:{}] {} {}'.format(executor_id[-4:], resource, state_name, message)
    
    def log(self, message):
        raise NotImplementedError
    
    def initialize(self):
        raise NotImplementedError

class LoggerFactory(object):
    def logger_factory(self, execution_id, executor_id):
        raise NotImplementedError

class TaskDispatcher(object):
    def dispatch(self, resource, input, context):
        raise NotImplementedError

