'''
Created on Oct 28, 2017

@author: bkehoe
'''

from __future__ import absolute_import

from . import executor

def decorator(handler_function):
    """Decorator to wrap a Lambda handler to enable execution as a state machine.
    Use like:
    @heaviside.decorator
    def handler(event, context):
        #proceed as normal
    """
    def wrapper(event, context):
        if not executor.is_heaviside_execution(event, context):
            return handler_function(event, context)
        
        state_machine = executor.StateMachine.hydrate(context)
        
        task_runner = lambda: handler_function(event, context)
        exception_handler = lambda e: 'States.TaskFailed'
        
        return state_machine.run_task(task_runner, exception_handler)

    wrapper.__name__ = handler_function.__name__
    return wrapper