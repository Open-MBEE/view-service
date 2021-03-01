# OpenMBEE View Service

The OpenMBEE View Service will enable a dramatic expansion of the capabilities of interactive View creation. Current features include
* incrementally and elastically extracting content from a [Confluence](https://www.atlassian.com/software/confluence) wiki, 
* enriching and transforming the data into an [RDF](https://www.w3.org/RDF/) graph by leveraging [OpenMBEE Confluence MDK](https://github.com/Open-MBEE/confluence-mdk),
* loading into a graph database, e.g. [Amazon Neptune](https://aws.amazon.com/neptune/), to enable graph queries, e.g. [SPARQL](https://www.w3.org/TR/sparql11-query/), on the dataset.

## Development
This service is designed as a higher-level service built on top of serverless and managed cloud services where the infrastructure is defined as code using [Cloud Development Kit](https://aws.amazon.com/cdk/).

The `cdk.json` file tells the CDK Toolkit how to execute your app. The build step is not required when using JavaScript.

### Useful commands

 * `npm run test`         perform the jest unit tests
 * `cdk deploy`           deploy this stack to your default AWS account/region
 * `cdk diff`             compare deployed stack with current state
 * `cdk synth`            emits the synthesized CloudFormation template
