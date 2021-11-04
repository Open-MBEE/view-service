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
    IntegrationPattern,
    JsonPath,
    Map,
    Pass,
    Result,
    StateMachine,
    TaskInput
} from '@aws-cdk/aws-stepfunctions';
import {EcsFargateLaunchTarget, EcsRunTask, LambdaInvoke} from '@aws-cdk/aws-stepfunctions-tasks';
import {Cluster, ContainerImage, FargatePlatformVersion, FargateTaskDefinition, LogDriver} from '@aws-cdk/aws-ecs';
import {Repository} from '@aws-cdk/aws-ecr';

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

        const neptuneLoadingRoleArn = scope.node.tryGetContext('stack:neptuneLoadingRoleArn');
        const neptuneConsumerSecurityGroupId = scope.node.tryGetContext('stack:neptuneConsumerSecurityGroupId');

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

        const secretArn = scope.node.tryGetContext('secret_arn');
        const secrets = Secret.fromSecretCompleteArn(this, 'ViewServiceSecrets', secretArn);

        const confluenceTokenKey = scope.node.tryGetContext('confluence_token_key');
        const confluenceUserKey = scope.node.tryGetContext('confluence_user_key');
        const confluencePassKey = scope.node.tryGetContext('confluence_pass_key');

        const notificationEndpointKey = scope.node.tryGetContext('notification_endpoint_key');
        const sparqlEndpointKey = scope.node.tryGetContext('sparql_endpoint_key');

        const confluenceToken = secrets.secretValueFromJson(confluenceTokenKey);
        const confluenceUser = secrets.secretValueFromJson(confluenceUserKey);
        const confluencePass = secrets.secretValueFromJson(confluencePassKey);
        const notificationEndpoint = secrets.secretValueFromJson(notificationEndpointKey);
        const neptuneEndpoint = secrets.secretValueFromJson(sparqlEndpointKey);

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

        const taskRole = createLambdaRole(this, 'ECSTaskRole', {
            managedPolicies: [
                ManagedPolicy.fromAwsManagedPolicyName(`service-role/AmazonEC2ContainerServiceforEC2Role`),
            ],
            assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com')
        });

        const taskExecutionRole = createLambdaRole(this, 'ECSTaskExecutionRole', {
            managedPolicies: [
                ManagedPolicy.fromAwsManagedPolicyName(`service-role/AmazonECSTaskExecutionRolePolicy`),
            ],
            assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com')
        });

        const ecsCluster = new Cluster(this, 'ViewServiceCluster', {
            vpc: vpc
        });

        const neptuneStagingBucket = new Bucket(this, 'NeptuneStagingBucket', {
            removalPolicy: RemovalPolicy.DESTROY
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

        let neptuneLoadingRole;
        try {
            neptuneLoadingRole = Role.fromRoleArn(this, 'ViewServiceNeptuneLoadingRole', neptuneLoadingRoleArn);
            neptuneLoadingRole.assumeRolePolicy?.addStatements(
                new PolicyStatement({
                    actions: ['sts:AssumeRole'],
                    effect: Effect.ALLOW,
                    principals: [new ServicePrincipal('rds.amazonaws.com')],
                })
            );
            neptuneLoadingRole.addToPrincipalPolicy(new PolicyStatement({
                effect: Effect.ALLOW,
                resources: [neptuneStagingBucket.bucketArn],
                actions: [
                    's3:List*',
                ],
            }));
            neptuneLoadingRole.addToPrincipalPolicy(new PolicyStatement({
                effect: Effect.ALLOW,
                resources: [`${neptuneStagingBucket.bucketArn}/*`],
                actions: [
                    's3:Get*',
                ],
            }));
        } catch (e) {
            neptuneLoadingRole = new Role(this, 'ViewServiceNeptuneLoadingRole', {
                assumedBy: new ServicePrincipal('rds.amazonaws.com'),
                ...rolePrefix && {roleName: `${rolePrefix}${this.stackName}NeptuneLoadingRole`},
                ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
            });
        }

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

        let neptuneConsumerSecurityGroup;
        try {
            neptuneConsumerSecurityGroup = SecurityGroup.fromSecurityGroupId(this, 'NeptuneConsumerSecurityGroup', neptuneConsumerSecurityGroupId, {
                mutable: false
            });
        } catch (e) {
            neptuneConsumerSecurityGroup = new SecurityGroup(this, 'NeptuneConsumerSecurityGroup', {
                vpc: vpc
            });
        }

        let resolvedEndpoint;
        if (!neptuneEndpoint) {
            const neptuneSubnetGroup = new CfnDBSubnetGroup(this, 'NeptuneSubnetGroup', {
                dbSubnetGroupName: `${this.stackName}NeptuneSubnetGroup`.toLowerCase(),
                dbSubnetGroupDescription: `${this.stackName}NeptuneSubnetGroup`,
                subnetIds: vpc.privateSubnets.map(subnet => subnet.subnetId),
            });

            const neptuneClusterSecurityGroup = new SecurityGroup(this, 'NeptuneClusterSecurityGroup', {
                vpc: vpc,
            });
            neptuneClusterSecurityGroup.addIngressRule(neptuneConsumerSecurityGroup, Port.tcp(8182));
            const neptuneCluster = new CfnDBCluster(this, 'NeptuneCluster', {
                dbClusterIdentifier: `${this.stackName}NeptuneCluster`.toLowerCase(),
                engineVersion: '1.0.4.1',
                associatedRoles: [
                    {
                        roleArn: neptuneLoadingRole.roleArn,
                    },
                ],
                dbSubnetGroupName: neptuneSubnetGroup.dbSubnetGroupName,
                vpcSecurityGroupIds: [neptuneClusterSecurityGroup.securityGroupId]
            });
            neptuneCluster.addDependsOn(neptuneSubnetGroup);
            const neptuneInstance = new CfnDBInstance(this, 'NeptuneInstance', {
                dbClusterIdentifier: neptuneCluster.dbClusterIdentifier,
                dbInstanceClass: 'db.r5.2xlarge',
            });
            neptuneInstance.addDependsOn(neptuneCluster);

            resolvedEndpoint = 'https://' + neptuneCluster.attrEndpoint + ':' + neptuneCluster.attrPort;
        } else {
            resolvedEndpoint = neptuneEndpoint;
        }

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
                'SPARQL_ENDPOINT': `${resolvedEndpoint}`,
            },
            // TODO right size
            timeout: Duration.minutes(15),
            securityGroups: [neptuneConsumerSecurityGroup],
        });

        const loadNeptuneGraphLambda = new NodejsFunction(this, 'LoadNeptuneGraph', {
            ...commonNodejsFunctionProps,
            handler: 'loadNeptuneGraph',
            environment: {
                'SPARQL_ENDPOINT': `${resolvedEndpoint}`,
                'NEPTUNE_S3_BUCKET_URL': `s3://${neptuneStagingBucket.bucketName}`,
                'NEPTUNE_S3_IAM_ROLE_ARN': neptuneLoadingRole.roleArn,
            },
            // TODO right size
            timeout: Duration.minutes(15),
            securityGroups: [neptuneConsumerSecurityGroup],
        });

        const sendNotificationLambda = new NodejsFunction(this, 'SendNotification', {
            ...commonNodejsFunctionProps,
            handler: 'sendNotification',
            environment: {
                'NOTIFICATION_ENDPOINT': `${notificationEndpoint}`,
            },
            timeout: Duration.minutes(2),
            role: vpcAccessLambdaRole,
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
                'graph.$': "States.Format('https://wiki.jpl.nasa.gov/display/{}', $.impl.space)",
            }),
            payloadResponseOnly: true,
            lambdaFunction: clearNeptuneGraphLambda,
        });

        const loadNeptuneGraphJob = new LambdaInvoke(this, 'LoadNeptuneGraphJob', {
            resultPath: '$.impl.neptune.load',
            payload: TaskInput.fromObject({
                // TODO Review IRI definition
                'graph.$': "States.Format('https://wiki.jpl.nasa.gov/display/{}', $.impl.space)",
                'prefix.$': '$.requestId',
            }),
            payloadResponseOnly: true,
            lambdaFunction: loadNeptuneGraphLambda,
        });

        const regenerateTaskDefinition = new FargateTaskDefinition(this, 'RegenerateDiagramTask', {
            executionRole: taskExecutionRole,
            taskRole: taskRole,
            memoryLimitMiB: 30720,
            cpu: 4096,
        });

        const containerDefinition = regenerateTaskDefinition.addContainer('RegenerateDiagramContainer', {
            image: ContainerImage.fromEcrRepository(Repository.fromRepositoryName(this, 'DiagramGeneratorRepository', 'ced-diagram-generator'), 'latest'),
            containerName: 'RegenerateDiagramContainer',
            essential: true,
            memoryLimitMiB: 30720,
            cpu: 4096,
            environment: {
                WIKI_USER: confluenceUser,
                WIKI_PASS: confluencePass,
                WIKI_SPACE: '',
                'NEPTUNE_ENDPOINT': `${resolvedEndpoint}/sparql`,
            },
            logging: LogDriver.awsLogs({
                streamPrefix: "RegenerateDiagram"
            })
        });

        const finish = new Choice(this, 'JobCompleteCheck')
            .when(Condition.stringEquals('$.impl.neptune.load.overallStatus.status', 'LOAD_COMPLETED'), new LambdaInvoke(this, 'SuccessNotification', {
                resultPath: '$.impl.notification.result',
                payload: TaskInput.fromObject({
                    'prefix.$': '$.requestId',
                    'space.$': '$.impl.space',
                    'status': 'Succeeded',
                    'job': 'Neptune Load'
                }),
                payloadResponseOnly: true,
                lambdaFunction: sendNotificationLambda,
            }))
            .when(Condition.isNotPresent('$.impl.neptune.load.overallStatus.status'), loadNeptuneGraphJob.addRetry({
                maxAttempts: 5
            }))
            .otherwise(new LambdaInvoke(this, 'FailureNotification', {
                resultPath: '$.impl.notification.result',
                payload: TaskInput.fromObject({
                    'prefix.$': '$.requestId',
                    'space.$': '$.impl.space',
                    'status': 'Failed',
                    'job': 'Neptune Load'
                }),
                payloadResponseOnly: true,
                lambdaFunction: sendNotificationLambda,
            }));

        copyOntologyJob
            .next(clearNeptuneGraphJob)
            .next(loadNeptuneGraphJob)
            .next(finish);

        const stateMachineRole = new Role(this, 'StateMachineRole', {
            assumedBy: new ServicePrincipal('states.amazonaws.com'),
            ...rolePrefix && {roleName: `${rolePrefix}${this.stackName}StateMachineRole`},
            ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
        });

        const viewServiceStateMachine = new StateMachine(this, 'ViewServiceStateMachine', {
            definition: stateMachineDefinition,
            role: stateMachineRole,
        });


        const regenerateDiagrams = new EcsRunTask(this, 'RegenerateDiagramsJob', {
            integrationPattern: IntegrationPattern.RUN_JOB,
            resultPath: '$.impl.diagrams.result',
            cluster: ecsCluster,
            taskDefinition: regenerateTaskDefinition,
            launchTarget:  new EcsFargateLaunchTarget({
                platformVersion: FargatePlatformVersion.LATEST
            }),
            containerOverrides: [
                {
                    containerDefinition: containerDefinition,
                    environment: [
                        {
                            name: 'WIKI_SPACE',
                            value: JsonPath.stringAt('$.space')
                        },
                        {
                            name: 'WIKI_SERVER',
                            value: JsonPath.stringAt('$.server')
                        }
                    ]
                }
            ]
        }).next(new LambdaInvoke(this, 'DiagramGenerationNotification', {
            payload: TaskInput.fromObject({
                'space.$': '$.space',
                //'status.$': '$.impl.diagrams.result.LastStatus',
                'status': 'Complete',
                'job': 'Diagram Generation'
            }),
            payloadResponseOnly: true,
            lambdaFunction: sendNotificationLambda,
        }));

        const diagramGenerationStateMachine = new StateMachine(this, 'DiagramGenerationStateMachine', {
            definition: regenerateDiagrams,
            role: stateMachineRole,
        });

        const stateMachineEventRole = new Role(this, 'StateMachineEventsRole', {
            assumedBy: new ServicePrincipal('events.amazonaws.com'),
            ...rolePrefix && {roleName: `${rolePrefix}${this.stackName}StateMachineEventsRole`},
            ...permissionsBoundaryPolicy && {permissionsBoundary: permissionsBoundaryPolicy},
        });
        const stateMachineEventPolicy = new Policy(this, 'StateMachineEventsPolicy', {
            statements: [
                new PolicyStatement({
                    effect: Effect.ALLOW,
                    resources: [viewServiceStateMachine.stateMachineArn, diagramGenerationStateMachine.stateMachineArn],
                    actions: [
                        'states:StartExecution'
                    ],
                }),
            ],
            roles: [stateMachineEventRole],
            ...policyPrefix && {policyName: `${policyPrefix}${this.stackName}StateMachineEventsPolicy`},
        });
    }
}

// module.exports = {ViewServiceStack};
