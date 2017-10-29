'''
Created on Oct 29, 2017

@author: bkehoe
'''

from __future__ import absolute_import

import threading
import time

from . import components

def create_components(executor_class, central_execution_store=False):
    definition_store = LocalDefinitionStore()
    
    if central_execution_store:
        execution_store = LocalExecutionCentralStore()
    else:
        execution_store = LocalExecutionContextStore()
    
    logger_factory = LocalLoggerFactory()
    
    
    task_dispatcher = LocalTaskDispatcher(executor_class,
                                           definition_store, 
                                           execution_store,
                                           logger_factory)
    
    
    return {
        "definition_store": definition_store,
        "execution_store": execution_store,
        "logger_factory": logger_factory,
        "task_dispatcher": task_dispatcher,
    }

class LocalDefinitionStore(components.DefinitionStore):
    def __init__(self):
        self.store = {}
    
    def get_context(self):
        return {}
    
    def hydrate(self, context):
        pass
    
    def put_anonymous(self, definition):
        hash = definition.get_hash()
        self.store[hash] = definition
        return hash
    
    def hydrate_definition(self, definition_id):
        return self.store[definition_id]


class LocalExecutionCentralStore(components.ExecutionStore):
    def __init__(self):
        self.store = {}
    
    def execution_factory(self, execution_id, definition_store):
        return self.Execution(execution_id, definition_store, self.store)
    
    class Execution(components.Execution):
        def __init__(self, execution_id, definition_store, store):
            components.Execution.__init__(self, execution_id, definition_store)
            self.store = None
    
        def get_context(self):
            return {}
        
        def hydrate(self, context):
            pass
        
        def initialize(self, definition):
            def_id = self.definition_store.put_anonymous(definition)
            
            self.store[self.execution_id] = {
                'current_state': None,
                'result': None,
                'definition_id': def_id,
            }
        
        def get_definition(self):
            return self.definition_store.hydrate_definition(self.store[self.execution_id]['definition_id'])
        
        def change_state(self, new_state_name, data=None):
            print 'changing state to', new_state_name
            self.store[self.execution_id]['current_state'] = components.State(new_state_name, data=data)
        
        def update_state_data(self, data):
            self.store[self.execution_id]['current_state'].data = data
        
        def set_result(self, result):
            print 'setting result'
            self.store[self.execution_id]['current_state'] = None
            self.store[self.execution_id]['result'] = result
        
        def get_current_state_and_result(self):
            data = self.store[self.execution_id]
            return (data['current_state'], data['result']) 

class LocalExecutionContextStore(components.ExecutionStore):
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

class LocalLogger(components.ExecutorComponent):
    def get_context(self):
        return {}
    
    def hydrate(self, context):
        pass
    
    def log(self, message):
        print message
    
    def initialize(self):
        pass

class LocalLoggerFactory(object):
    def logger_factory(self, execution_id, executor_id):
        return LocalLogger()

class LocalTaskDispatcher(components.TaskDispatcher):
    def __init__(self, executor_class,
               definition_store,
               execution_store,
               logger_factory):
        self.executor_class = executor_class
        
        self.definition_store=definition_store
        self.execution_store=execution_store
        self.logger_factory=logger_factory
    
    def dispatch(self, resource, input, context):
        def task_thread():
            executor_kwargs = {
               "context": context,
               "definition_store": self.definition_store,
               "execution_store": self.execution_store,
               "logger_factory": self.logger_factory,
               "task_dispatcher": self,
            }
            
            print '\ntask thread started for resource', resource
            executor = self.executor_class.hydrate(**executor_kwargs)
            
            task_runner = lambda: time.sleep(2)
            exception_handler = lambda e: 'States.TaskFailed'
            
            executor.run_task(task_runner, exception_handler)
            print 'task thread finished for resource', resource
        
        threading.Thread(target=task_thread).start()
