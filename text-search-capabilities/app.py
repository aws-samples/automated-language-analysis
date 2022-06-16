#!/usr/bin/env python3


import aws_cdk as cdk

from cdk.main import LanguageAnalysisStack


__STACK_DESCRIPTION = 'This stack includes resources needed to deploy a fully automated language analysis pipeline. \
Resources are organised in different nested stacks, each of them representing a stage of the pipeline.'


app = cdk.App()
LanguageAnalysisStack(app, "LanguageAnalysis", stack_name='LanguageAnalysis', description=__STACK_DESCRIPTION)

app.synth()
