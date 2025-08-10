import aws_cdk as core
import aws_cdk.assertions as assertions

from medlaunch_elt.medlaunch_elt_stack import MedlaunchEltStack

# example tests. To run these tests, uncomment this file along with the example
# resource in medlaunch_elt/medlaunch_elt_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MedlaunchEltStack(app, "medlaunch-elt")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
