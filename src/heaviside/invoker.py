'''
Lambda handler that accepts a request in the format:
{
  "StateMachine": <json definition>,
  "Input": <input to the state machine
}

starts the state machine and returns an id for it.

Created on Oct 28, 2017

@author: bkehoe
'''

from __future__ import absolute_import

import json
import os

import boto3

from . import executor, aws

def handler(event, context):
    """Create the state machine from the definition and dispatch. Return the id."""
    
    definition = event["StateMachine"]
    if isinstance(definition, basestring):
        definition = json.loads(definition)
        
    definition_bucket_name = os.environ["StateMachineBucket"]
    state_table_name = os.environ["StateTable"]
    
    components = aws.create_and_configure_components(definition_bucket_name)
    
    ex = executor.Executor.create(definition, **components)

    ex.dispatch(event["Input"])
    
    return {
        "id": ex.id,
    }