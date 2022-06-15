# automated-language-analysis
This repository contains a CDK project that deploys a fully automated language analysis pipeline.

## In this page

- [Analysis deep dive](#AnalysisDeepDive)
- [UseCases](#UseCases)
- [Prerequisites](#Prerequisites)
- [Security](#Security)
- [License](#License)

## Analysis deep dive

The application performs a language analysis of the loaded documents divided into the following categories:

### Foreign words

This part of the analysis calculates:

- Rate of use of foreign words
- Most frequently occurring foreign words

### Lexical richness

This one is calculated in base of different parameters:

#### Variability
- TTR: text-to-word ratio: ratio of distinct words in a document to the total number of words in a document.
- MLTD: measurement of textual lexical diversity.

### Errors

The following type of errors are calculated:

- Typographical / punctuation
- Grammatical
- Writing style
- Inconsistency
- Spelling

## Use cases

Depending on your needs and use case, there are two variants of the application that you can deploy: the first one indexes documents in Amazon OpenSearch service to offer text-based search. The second bypasses Amazon OpenSearch service and makes use of Amazon QuickSight for displaying the results.

- [Automated language analysis with text search capabilities](#)
- Automated language analysis with QuickSight integration (under development)

Select the variant that best suits your use case to view its documentation and deployment instructions.

## Prerequisites

The application is deployed to your AWS account via CDK, so you will need to install it in order to continue with the deployment. Execute the following command to install it:

```bash
npm install -g aws-cdk
```

You can read more about getting started with CDK by reading the [AWS Cloud Development Kit developer guide](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
