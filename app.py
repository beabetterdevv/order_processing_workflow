#!/usr/bin/env python3

import aws_cdk as cdk

from order_processing_workflow.order_processing_workflow_stack import OrderProcessingWorkflowStack


app = cdk.App()
OrderProcessingWorkflowStack(app, "order-processing-workflow")

app.synth()
