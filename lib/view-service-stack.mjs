import {Duration, RemovalPolicy, Stack} from '@aws-cdk/core';
import {Port, SecurityGroup, Vpc} from '@aws-cdk/aws-ec2';
import {Effect, ManagedPolicy, Policy, PolicyStatement, Role, ServicePrincipal} from '@aws-cdk/aws-iam';
import {Runtime} from '@aws-cdk/aws-lambda';
import {CfnDBCluster, CfnDBInstance, CfnDBSubnetGroup} from '@aws-cdk/aws-neptune';
import {NodejsFunction} from '@aws-cdk/aws-lambda-nodejs';
import {Bucket} from '@aws-cdk/aws-s3';
import {BucketDeployment, Source} from '@aws-cdk/aws-s3-deployment';
import {Secret} from '@aws-cdk/aws-secretsmanager';
import {
    Choice,
    Condition,
    Map,
    Pass,
    Result,
    StateMachine,
    Succeed,
    TaskInput,
} from '@aws-cdk/aws-stepfunctions';
import {LambdaInvoke} from '@aws-cdk/aws-stepfunctions-tasks';

import path from 'path';
import {createRequire} from 'module';
const require = createRequire(import.meta.url);

export class ViewServiceStack extends Stack {
    /**
     *
     * @param {Construct} scope
     * @param {string} id
     * @param {StackProps=} props
     */
    constructor(scope, id, props) {
        super(scope, id, props);

        const rolePrefix = scope.node.tryGetContext('stack:rolePrefix');
        const policyPrefix = scope.node.tryGetContext('stack:policyPrefix');
        const permissionsBoundary = scope.node.tryGetContext('stack:permissionsBoundary');
        const vpcId = scope.node.tryGetContext('stack:vpcId');

        const permissionsBoundaryPolicy = permissionsBoundary ? ManagedPolicy.fromManagedPolicyName(this, permissionsBoundary, permissionsBoundary) : null;
        const vpc = vpcId ? Vpc.fromLookup(this, 'ViewServiceVpc', {vpcId: vpcId}) : null;

        if(!vpc) {
            throw new Error(`Invalid stack:vpcId context value: ${vpcId}`);
        }

        const basicLambdaManagedPolicies = [
            ManagedPolicy.fromAwsManagedPolicyName(`service-role/AWSLambdaBasicExecutionRole`),
        ];
        const vpcAccessLambdaManagedPolicies = basicLambdaManagedPolicies.concat([
            ManagedPolicy.fromAwsManagedPolicyName(`service-role/AWSLambdaVPCAccessExecutionRole`),
        ]);

        const confluenceToken = Secret.fromSecretAttributes(this, scope.node.tryGetContext('confluence_secret_key'), {
            secretArn: scope.node.tryGetContext('confluence_secret_arn')
            // If the secret is encrypted using a KMS-hosted CMK, either import or reference that key:
            // encryptionKey: ...
        });

        /**
         *
         * @param {Stack} stack
         * @param {string} id
         * @param {RoleProps=} props
         */
        const createLambdaRole = function(stack, roleId, roleProps) {
            return new Role(stack, roleId, {
                assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
                ...rolePrefix && {roleName: `${rolePrefix}${stack.stackName}${roleId}`},
                ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
                ...roleProps,
            });
        };

        const basicLambdaRole = createLambdaRole(this, 'BasicLambdaRole', {
            managedPolicies: basicLambdaManagedPolicies,
        });
        const vpcAccessLambdaRole = createLambdaRole(this, 'VPCAccessLambdaRole', {
            managedPolicies: vpcAccessLambdaManagedPolicies,
        });

        const neptuneStagingBucket = new Bucket(this, 'NeptuneStagingBucket', {
            removalPolicy: RemovalPolicy.DESTROY,
            // TODO lifecycleRules to clean up old files
        });

        const staticStagingDeployment = new BucketDeployment(this, 'StaticStagingDeployment', {
            sources: [
                Source.asset(path.dirname(require.resolve('confluence-mdk/src/asset/ontology.ttl')), {
                    exclude: ['**', '!ontology.ttl'],
                }),
            ],
            destinationBucket: neptuneStagingBucket,
            destinationKeyPrefix: 'static',
            role: basicLambdaRole,
        });

        const neptuneLoadingRole = new Role(this, 'NeptuneLoadingRole', {
            assumedBy: new ServicePrincipal('rds.amazonaws.com'),
            ...rolePrefix && {roleName: `${rolePrefix}${this.stackName}NeptuneLoadingRole`},
            ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
        });
        const neptuneLoadingPolicy = new Policy(this, 'NeptuneLoadingPolicy', {
            statements: [
                // ref: https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html
                new PolicyStatement({
                    effect: Effect.ALLOW,
                    resources: [neptuneStagingBucket.bucketArn],
                    actions: [
                        's3:List*',
                    ],
                }),
                new PolicyStatement({
                    effect: Effect.ALLOW,
                    resources: [`${neptuneStagingBucket.bucketArn}/*`],
                    actions: [
                        's3:Get*',
                    ],
                }),
            ],
            roles: [neptuneLoadingRole],
            ...policyPrefix && {policyName: `${policyPrefix}${this.stackName}NeptuneLoadingPolicy`},
        });
        const neptuneSubnetGroup = new CfnDBSubnetGroup(this, 'NeptuneSubnetGroup', {
            dbSubnetGroupName: `${this.stackName}NeptuneSubnetGroup`.toLowerCase(),
            dbSubnetGroupDescription: `${this.stackName}NeptuneSubnetGroup`,
            subnetIds: vpc.privateSubnets.map(subnet => subnet.subnetId),
        });
        const neptuneClusterSecurityGroup = new SecurityGroup(this, 'NeptuneClusterSecurityGroup', {
            vpc: vpc,
        });
        const neptuneConsumerSecurityGroup = new SecurityGroup(this, 'NeptuneConsumerSecurityGroup', {
            vpc: vpc,
        });
        neptuneClusterSecurityGroup.addIngressRule(neptuneConsumerSecurityGroup, Port.tcp(8182));
        // TODO Define parameter group
        const neptuneCluster = new CfnDBCluster(this, 'NeptuneCluster', {
            dbClusterIdentifier: `${this.stackName}NeptuneCluster`.toLowerCase(),
            engineVersion: '1.0.4.1',
            associatedRoles: [
                {
                    roleArn: neptuneLoadingRole.roleArn,
                },
            ],
            dbSubnetGroupName: neptuneSubnetGroup.dbSubnetGroupName,
            vpcSecurityGroupIds: [neptuneClusterSecurityGroup.securityGroupId],
        });
        neptuneCluster.addDependsOn(neptuneSubnetGroup);
        const neptuneInstance = new CfnDBInstance(this, 'NeptuneInstance', {
            dbClusterIdentifier: neptuneCluster.dbClusterIdentifier,
            dbInstanceClass: 'db.r5.2xlarge',
        });
        neptuneInstance.addDependsOn(neptuneCluster);

        const commonNodejsFunctionProps = {
            runtime: Runtime.NODEJS_14_X,
            entry: 'src/functions.js',
            role: vpcAccessLambdaRole,
            bundling: {
                target: 'es2020',
                format: 'esm',
            },
            ...vpc && {vpc: vpc},
        };
        const neptuneStagingPutLambdaRole = createLambdaRole(this, 'NeptuneStagingPutLambdaRole', {
            managedPolicies: vpcAccessLambdaManagedPolicies,
        });
        const neptuneStagingPutLambdaPolicy = new Policy(this, 'NeptuneStagingPutLambdaPolicy', {
            statements: [
                new PolicyStatement({
                    effect: Effect.ALLOW,
                    resources: [neptuneStagingBucket.bucketArn],
                    actions: [
                        's3:ListBucket',
                    ],
                }),
                new PolicyStatement({
                    effect: Effect.ALLOW,
                    resources: [`${neptuneStagingBucket.bucketArn}/*`],
                    actions: [
                        's3:PutObject',
                        's3:GetObject',
                    ],
                }),
            ],
            roles: [neptuneStagingPutLambdaRole],
            ...policyPrefix && {policyName: `${policyPrefix}${this.stackName}NeptuneStagingPutLambdaPolicy`},
        });

        const generateUuidLambda = new NodejsFunction(this, 'GenerateUuid', {
            ...commonNodejsFunctionProps,
            handler: 'generateUuid',
        });

        const exportConfluenceLambda = new NodejsFunction(this, 'ExportConfluence', {
            ...commonNodejsFunctionProps,
            handler: 'exportConfluencePage',
            environment: {
                'CONFLUENCE_TOKEN': confluenceToken,
                'S3_BUCKET': neptuneStagingBucket.bucketName,
            },
            // TODO right size
            timeout: Duration.minutes(1),
            role: neptuneStagingPutLambdaRole,
        });

        const popConfluencePageTreeLambda = new NodejsFunction(this, 'PopConfluencePageTree', {
            ...commonNodejsFunctionProps,
            handler: 'popConfluencePageTree',
            environment: {
                'CONFLUENCE_TOKEN': confluenceToken,
            },
            // TODO right size
            timeout: Duration.minutes(1),
            role: vpcAccessLambdaRole,
        });

        const flattenArrayLambda = new NodejsFunction(this, 'FlattenArray', {
            ...commonNodejsFunctionProps,
            handler: 'flattenArray',
        });

        const copyS3ObjectLambda = new NodejsFunction(this, 'CopyS3Object', {
            ...commonNodejsFunctionProps,
            handler: 'copyS3Object',
            environment: {
                'S3_BUCKET': neptuneStagingBucket.bucketName,
            },
            // TODO right size
            timeout: Duration.minutes(1),
            role: neptuneStagingPutLambdaRole,
        });

        const clearNeptuneGraphLambda = new NodejsFunction(this, 'ClearNeptuneGraph', {
            ...commonNodejsFunctionProps,
            handler: 'clearNeptuneGraph',
            environment: {
                'SPARQL_ENDPOINT': `https://${neptuneCluster.attrEndpoint}:8182`,
            },
            // TODO right size
            timeout: Duration.minutes(15),
            securityGroups: [neptuneConsumerSecurityGroup],
        });

        const loadNeptuneGraphLambda = new NodejsFunction(this, 'LoadNeptuneGraph', {
            ...commonNodejsFunctionProps,
            handler: 'loadNeptuneGraph',
            environment: {
                'SPARQL_ENDPOINT': `https://${neptuneCluster.attrEndpoint}:8182`,
                'NEPTUNE_S3_BUCKET_URL': `s3://${neptuneStagingBucket.bucketName}`,
                'NEPTUNE_S3_IAM_ROLE_ARN': neptuneLoadingRole.roleArn,
            },
            // TODO right size
            timeout: Duration.minutes(15),
            securityGroups: [neptuneConsumerSecurityGroup],
        });

        const generateRequestIdJob = new LambdaInvoke(this, 'GenerateRequestIdJob', {
            resultPath: '$.requestId',
            // TODO Replace with ResultSelector when supported in CDK, ref: https://github.com/aws/aws-cdk/issues/9904
            payloadResponseOnly: true,
            lambdaFunction: generateUuidLambda,
        });

        const exportConfluenceJob = new LambdaInvoke(this, 'ExportConfluenceJob', {
            resultPath: '$.export',
            // TODO Replace with ResultSelector when supported in CDK, ref: https://github.com/aws/aws-cdk/issues/9904
            payloadResponseOnly: true,
            lambdaFunction: exportConfluenceLambda,
        });

        const initializeStackPass = new Pass(this, 'InitializeStackPass', {
            inputPath: '$',
            resultPath: '$.impl',
        });

        const initializeResultPass = new Pass(this, 'InitializeResultPass', {
            result: Result.fromArray([]),
            resultPath: '$.result',
        });

        const popConfluencePageTreeJob = new LambdaInvoke(this, 'PopConfluencePageTreeJob', {
            inputPath: '$.impl',
            resultPath: '$.impl',
            // TODO Replace with ResultSelector when supported in CDK, ref: https://github.com/aws/aws-cdk/issues/9904
            payloadResponseOnly: true,
            lambdaFunction: popConfluencePageTreeLambda,
        });

        const mapState = new Map(this, 'StateMachineMap', {
            itemsPath: '$.impl.result',
            resultPath: '$.impl.result',
            parameters: {
                'requestId.$': '$.requestId',
                'page.$': '$$.Map.Item.Value',
            },
        });
        mapState.iterator(exportConfluenceJob);

        const aggregateMapResultJob = new LambdaInvoke(this, 'AggregateMapResultJob', {
            resultPath: '$.result',
            payload: TaskInput.fromObject({
                'args.$': 'States.Array($.result, $.impl.result)',
            }),
            payloadResponseOnly: true,
            lambdaFunction: flattenArrayLambda,
        });

        const stackEmptyChoice = new Choice(this, 'StackEmptyChoice')
            .otherwise(popConfluencePageTreeJob);

        const stateMachineDefinition = generateRequestIdJob
            .next(initializeStackPass)
            .next(initializeResultPass)
            .next(popConfluencePageTreeJob)
            .next(mapState)
            .next(aggregateMapResultJob)
            .next(stackEmptyChoice);

        const copyOntologyJob = new LambdaInvoke(this, 'CopyOntologyJob', {
            resultPath: '$.impl.ontology.upload',
            payload: TaskInput.fromObject({
                'source': 'static/ontology.ttl',
                'target.$': "States.Format('{}/ontology.ttl', $.requestId)",
            }),
            payloadResponseOnly: true,
            lambdaFunction: copyS3ObjectLambda,
        });
        stackEmptyChoice.when(Condition.isNotPresent('$.impl.stack[0]'), copyOntologyJob);

        const clearNeptuneGraphJob = new LambdaInvoke(this, 'ClearNeptuneGraphJob', {
            resultPath: '$.impl.neptune.clear',
            payload: TaskInput.fromObject({
                // TODO Review IRI definition
                'graph.$': "States.Format('https://openmbee.org/rdf/graph/{}', $.requestId)",
            }),
            payloadResponseOnly: true,
            lambdaFunction: clearNeptuneGraphLambda,
        });

        const loadNeptuneGraphJob = new LambdaInvoke(this, 'LoadNeptuneGraphJob', {
            resultPath: '$.impl.neptune.load',
            payload: TaskInput.fromObject({
                // TODO Review IRI definition
                'graph.$': "States.Format('https://openmbee.org/rdf/graph/{}', $.requestId)",
                'prefix.$': '$.requestId',
            }),
            payloadResponseOnly: true,
            lambdaFunction: loadNeptuneGraphLambda,
        });

        const success = new Succeed(this, 'Success');

        copyOntologyJob
            .next(clearNeptuneGraphJob)
            .next(loadNeptuneGraphJob)
            .next(success);

        const stateMachineRole = new Role(this, 'StateMachineRole', {
            assumedBy: new ServicePrincipal('states.amazonaws.com'),
            ...rolePrefix && {roleName: `${rolePrefix}${this.stackName}StateMachineRole`},
            ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
        });
        const viewServiceStateMachine = new StateMachine(this, 'ViewServiceStateMachine', {
            definition: stateMachineDefinition,
            role: stateMachineRole,
        });
    }
}

// module.exports = {ViewServiceStack};
