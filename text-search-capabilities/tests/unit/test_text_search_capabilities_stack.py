import aws_cdk as core
import aws_cdk.assertions as assertions

from text_search_capabilities.text_search_capabilities_stack import TextSearchCapabilitiesStack

# example tests. To run these tests, uncomment this file along with the example
# resource in text_search_capabilities/text_search_capabilities_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TextSearchCapabilitiesStack(app, "text-search-capabilities")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
