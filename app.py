#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_cdk import App, Environment
from medlaunch_elt.medlaunch_elt_stack import MedlaunchEltStack


app = cdk.App()

env_us_east_1 = Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="us-east-1"
)

MedlaunchEltStack(app, "MedlaunchEltStack", env=env_us_east_1)

app.synth()
