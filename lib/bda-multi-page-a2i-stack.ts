//
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
//
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ecr from 'aws-cdk-lib/aws-ecr-assets'
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as sagemaker from "aws-cdk-lib/aws-sagemaker";
import * as bedrock from "aws-cdk-lib/aws-bedrock";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as path from "path";
import * as fs from "fs";

export class BdaMultiPageA2IStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Get Account ID and Region
    const { accountId, region } = new cdk.ScopedAws(this);

    // Parameters that can be provided to the template
    const confidenceScoreParam = new cdk.CfnParameter(this, "ConfidenceScore", {
      type: "Number",
      description: "The confidence score which data below this level creates a human review",
      default: 0.70,
    });

    // Parameters for existing resources
    const existingUserPoolIdParam = new cdk.CfnParameter(this, "ExistingUserPoolId", {
      type: "String",
      description: "The user pool ID of the existing workforce (leave empty to create new)",
      default: "",
    });

    const existingClientIdParam = new cdk.CfnParameter(this, "ExistingClientId", {
      type: "String",
      description: "The client ID of the existing workforce (leave empty to create new)",
      default: "",
    });

    const workforceNameParam = new cdk.CfnParameter(this, "WorkforceName", {
      type: "String",
      description: "The name of the workforce",
      default: "bda-workforce",
    });

    // Check if we're using existing resources
    const existingUserPoolId = existingUserPoolIdParam.valueAsString;
    const existingClientId = existingClientIdParam.valueAsString;
    const workforceName = workforceNameParam.valueAsString;

    // Check if parameters were explicitly provided via CLI or context
    const useExistingResources = this.node.tryGetContext('ExistingUserPoolId') !== undefined &&
      this.node.tryGetContext('ExistingClientId') !== undefined;

    // Variables to store user pool, client ID, and workforce ARN
    let userPool: cognito.IUserPool;
    let userPoolClientId: string;
    let userPoolId: string;
    let workteamArn: string;

    if (useExistingResources) {
      // Use existing resources
      userPool = cognito.UserPool.fromUserPoolId(this, "ExistingUserPool", existingUserPoolId);
      userPoolClientId = existingClientId;
      userPoolId = existingUserPoolId;

      // Just reference the existing workforce by ARN, don't create a new one
      workteamArn = `arn:aws:sagemaker:${region}:${accountId}:workteam/private-crowd/${workforceName}`;
    } else {
      // Create new resources
      // Create the cognito user pool
      const newUserPool = new cognito.UserPool(this, "BdaUserPool", {
        userPoolName: "bda-user-pool",
        selfSignUpEnabled: true,
        signInCaseSensitive: false,
        signInAliases: {
          email: true,
          username: false,
        },
        passwordPolicy: {
          minLength: 8,
          requireLowercase: true,
          requireDigits: true,
          requireUppercase: true,
          requireSymbols: true,
        },
      });

      // Add a group to the pool
      newUserPool.addGroup("BdaUserPoolGroup", {
        groupName: "bda-user-pool-group",
      });

      // Add the domain for the user pool, using the account number as part of the domain name
      // in order to deal with cases where people may already have a pool defined.
      newUserPool.addDomain("BdaUserPoolDomain", {
        cognitoDomain: {
          domainPrefix: `bdadomain-${cdk.Aws.ACCOUNT_ID}`,
        },
      });

      // Create the user pool client
      const newUserPoolClient = new cognito.UserPoolClient(this, "BdaUserPoolClient", {
        userPoolClientName: "bda-user-pool-client",
        userPool: newUserPool,
        authFlows: {
          userPassword: true,
          userSrp: true,
        },
        generateSecret: true,
      });

      // Create the private workforce for sage maker ground truth using the client and user pool
      const workforce = new sagemaker.CfnWorkteam(this, "BdaWorkforce", {
        workteamName: workforceName,
        memberDefinitions: [{
          cognitoMemberDefinition: {
            cognitoClientId: newUserPoolClient.userPoolClientId,
            cognitoUserGroup: 'bda-user-pool-group',
            cognitoUserPool: newUserPool.userPoolId,
          }
        }],
        workforceName: workforceName,
        description: workforceName,
      });

      userPool = newUserPool;
      userPoolClientId = newUserPoolClient.userPoolClientId;
      userPoolId = newUserPool.userPoolId;
      workteamArn = `arn:aws:sagemaker:${region}:${accountId}:workteam/private-crowd/${workforceName}`;
    }

    // Create the input and output buckets
    const bdaInputBucket = new s3.Bucket(this, "BdaInputBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      eventBridgeEnabled: true
    });

    const bdaOutputBucket = new s3.Bucket(this, "BdaOutputBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true
    });

    // Create a role to allow for SageMaker Ground Truth and A2I to interact with 
    // the created data in the output bucket
    const a2iRole = new iam.Role(this, "A2iRole", {
      assumedBy: new iam.ServicePrincipal("sagemaker.amazonaws.com"),
    });

    // Allow for S3 actions
    bdaOutputBucket.grantReadWrite(a2iRole);

    // All for the workforce to create flows and human tasks
    a2iRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "sagemaker:CreateFlowDefinition",
          "sagemaker:CreateHumanTaskUi",
          "sagemaker:StartHumanLoop",
          "sagemaker:Describe*",
        ],
        resources: [
          workteamArn
        ],
      }));

    // Create the blueprints for the project
    // 1. Driver's License Blueprint
    const dlFileContent = JSON.parse(fs.readFileSync(path.join(__dirname, "../out/src/bda-blueprints/us_drivers_licenses.json"), "utf-8"));
    const dlBlueprint = new bedrock.CfnBlueprint(this, "DriversLicenseBlueprint", {
      blueprintName: "drivers-license-blueprint",
      type: "DOCUMENT",
      schema: dlFileContent
    });
    const dlBlueprintItem: bedrock.CfnDataAutomationProject.BlueprintItemProperty = {
      blueprintArn: dlBlueprint.attrBlueprintArn
    };

    // 2. Health Insurance Claim Form Blueprint
    const healthInsuranceClaimContent = JSON.parse(fs.readFileSync(path.join(__dirname, "../out/src/bda-blueprints/health_insurance_claim_form.json"), "utf-8"));
    const healthInsuranceClaimBlueprint = new bedrock.CfnBlueprint(this, "HealthInsuranceClaimBlueprint", {
      blueprintName: "health-insurance-claim-blueprint",
      type: "DOCUMENT",
      schema: healthInsuranceClaimContent
    });
    const healthInsuranceClaimBlueprintItem: bedrock.CfnDataAutomationProject.BlueprintItemProperty = {
      blueprintArn: healthInsuranceClaimBlueprint.attrBlueprintArn
    };

    // 3. Medical Insurance Cards Blueprint
    const medicalInsuranceCardsContent = JSON.parse(fs.readFileSync(path.join(__dirname, "../out/src/bda-blueprints/medical_insurance_cards.json"), "utf-8"));
    const medicalInsuranceCardsBlueprint = new bedrock.CfnBlueprint(this, "MedicalInsuranceCardsBlueprint", {
      blueprintName: "medical-insurance-cards-blueprint",
      type: "DOCUMENT",
      schema: medicalInsuranceCardsContent
    });
    const medicalInsuranceCardsBlueprintItem: bedrock.CfnDataAutomationProject.BlueprintItemProperty = {
      blueprintArn: medicalInsuranceCardsBlueprint.attrBlueprintArn
    };

    // 4. Surgical Pathology Report Blueprint
    const surgicalPathologyReportContent = JSON.parse(fs.readFileSync(path.join(__dirname, "../out/src/bda-blueprints/surgical_pathology_report.json"), "utf-8"));
    const surgicalPathologyReportBlueprint = new bedrock.CfnBlueprint(this, "SurgicalPathologyReportBlueprint", {
      blueprintName: "surgical-pathology-report-blueprint",
      type: "DOCUMENT",
      schema: surgicalPathologyReportContent
    });
    const surgicalPathologyReportBlueprintItem: bedrock.CfnDataAutomationProject.BlueprintItemProperty = {
      blueprintArn: surgicalPathologyReportBlueprint.attrBlueprintArn
    };

    // 5. Bank Statement Blueprint
    const bankStatementContent = JSON.parse(fs.readFileSync(path.join(__dirname, "../out/src/bda-blueprints/bank_statement.json"), "utf-8"));
    const bankStatementBlueprint = new bedrock.CfnBlueprint(this, "BankStatementBlueprint", {
      blueprintName: "bank-statement-blueprint",
      type: "DOCUMENT",
      schema: bankStatementContent
    });
    const bankStatementBlueprintItem: bedrock.CfnDataAutomationProject.BlueprintItemProperty = {
      blueprintArn: bankStatementBlueprint.attrBlueprintArn
    };

    // Create the Bedrock Data Automation Project and Profile
    const bdaProject = new bedrock.CfnDataAutomationProject(this, "BdaProject", {
      projectName: "bda-project",
      projectDescription: "Bedrock Data Automation Project Example",
      customOutputConfiguration: {
        blueprints: [
          dlBlueprintItem,
          healthInsuranceClaimBlueprintItem,
          medicalInsuranceCardsBlueprintItem,
          surgicalPathologyReportBlueprintItem,
          bankStatementBlueprintItem
        ],
      },
      standardOutputConfiguration: {
        // NOTE: This is a workaround as without the empty parameter the creation will fail for the project
      },
      overrideConfiguration: {
        document: {
          splitter: {
            state: 'ENABLED',
          },
        },
      },
    });

    // Create logs for the Bedrock Data Automation Classifier Lambda
    const bdaClassifierLogs = new logs.LogGroup(this, 'BdaClassifierLogGroup', {
      logGroupName: '/aws/lambda/bda-multi-page-a2i/bda-classifier-lambda-function',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create the lambda that is used to perform Bedrock Data Automation (BDA) Classification
    const bdaClassifierLambda = new lambda.DockerImageFunction(this, "BdaClassifierLambda", {
      description: "Lambda to perform Bedrock Data Automation Classification",
      logGroup: bdaClassifierLogs,
      timeout: cdk.Duration.seconds(900),
      retryAttempts: 0,
      memorySize: 2048,
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, "../out/src/bda-document-classifier"), {
        platform: ecr.Platform.LINUX_AMD64
      }),
      environment: {
        BDA_WORKTEAM_ARN: workteamArn,
        BDA_OUTPUT_BUCKET: bdaOutputBucket.bucketName,
        BDA_INPUT_BUCKET: bdaInputBucket.bucketName,
        ROLE_ARN: a2iRole.roleArn,
        CONFIDENCE_THRESHOLD: confidenceScoreParam.valueAsString,
        TEMPLATE_FILENAME: 'bda_template_multi.html'
      }
    });

    // Create logs for the Bedrock Data Automation Classifier Lambda
    const bdaProcessorLogs = new logs.LogGroup(this, 'BdaProcessorLogGroup', {
      logGroupName: '/aws/lambda/bda-multi-page-a2i/bda-processor-lambda-function',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create the lambda that is used to perform Bedrock Data Automation (BDA) Classification
    const bdaProcessorLambda = new lambda.DockerImageFunction(this, "BdaProcessorLambda", {
      description: "Lambda to perform Bedrock Data Automation Document Processor",
      logGroup: bdaProcessorLogs,
      timeout: cdk.Duration.seconds(900),
      retryAttempts: 0,
      memorySize: 512,
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, "../out/src/bda-document-processor"), {
        platform: ecr.Platform.LINUX_AMD64
      }),
      environment: {
        BDA_OUTPUT_BUCKET: bdaOutputBucket.bucketName,
        DATA_AUTOMATION_PROJECT_ARN: bdaProject.attrProjectArn,
        DATA_AUTOMATION_PROFILE_ARN: `arn:aws:bedrock:${region}:${accountId}:data-automation-profile/us.data-automation-v1`
      }
    });

    // Create logs for the Bedrock Data Automation A2I Aggregator Lambda
    const bdaA2IAggregatorLogs = new logs.LogGroup(this, 'BdaA2IAggregatorLogGroup', {
      logGroupName: '/aws/lambda/bda-multi-page-a2i/bda-a2i-aggregator-lambda-function',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create the lambda for A2I result aggregation
    const bdaA2IAggregatorLambda = new lambda.DockerImageFunction(this, "BdaA2IAggregatorLambda", {
      description: "Lambda to aggregate A2I review results",
      logGroup: bdaA2IAggregatorLogs,
      timeout: cdk.Duration.seconds(900),
      retryAttempts: 0,
      memorySize: 512,
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, "../out/src/bda-a2i-aggregator"), {
        platform: ecr.Platform.LINUX_AMD64
      }),
      environment: {
        BDA_OUTPUT_BUCKET: bdaOutputBucket.bucketName
      }
    });

    // Grant S3 permissions to the Lambda
    bdaOutputBucket.grantReadWrite(bdaA2IAggregatorLambda);

    // Add S3 event notifications for PUT operation
    bdaOutputBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.LambdaDestination(bdaA2IAggregatorLambda),
      { prefix: 'a2i-output/bda-review-flow-definition/', suffix: '.json' }
    );

    // Add S3 event notifications for POST operation
    bdaOutputBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_POST,
      new s3n.LambdaDestination(bdaA2IAggregatorLambda),
      { prefix: 'a2i-output/bda-review-flow-definition/', suffix: '.json' }
    );

    // Add S3 event notifications for COPY operation
    bdaOutputBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_COPY,
      new s3n.LambdaDestination(bdaA2IAggregatorLambda),
      { prefix: 'a2i-output/bda-review-flow-definition/', suffix: '.json' }
    );

    // Output the Lambda ARN for reference
    new cdk.CfnOutput(this, "BdaA2IAggregatorLambdaArn", {
      value: bdaA2IAggregatorLambda.functionArn,
      description: "ARN of the BDA A2I Aggregator Lambda function"
    });

    // Allow access to read and write to the buckets
    bdaInputBucket.grantReadWrite(bdaProcessorLambda);
    bdaOutputBucket.grantReadWrite(bdaProcessorLambda);
    bdaInputBucket.grantReadWrite(bdaClassifierLambda);
    bdaOutputBucket.grantReadWrite(bdaClassifierLambda);

    // Add SageMaker and A2I Runtime permissions to the Lambda role
    bdaClassifierLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'sagemaker:CreateHumanTaskUi',
        'sagemaker:DescribeHumanTaskUi',
        'sagemaker:CreateFlowDefinition',
        'sagemaker:DescribeFlowDefinition',
        'sagemaker:DeleteFlowDefinition',
        'sagemaker:DeleteHumanTaskUi',
        'sagemaker:DescribeHumanLoop',
        'sagemaker:StartHumanLoop',
        'iam:PassRole'
      ],
      resources: ['*']
    }));

    // Add permissions to the lambdas to allow for interaction with BDA resources
    bdaProcessorLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        "bedrock:InvokeDataAutomationAsync",
      ],
      resources: [
        `arn:aws:bedrock:us-west-2:${accountId}:data-automation-profile/us.data-automation-v1`,
        `arn:aws:bedrock:us-west-1:${accountId}:data-automation-profile/us.data-automation-v1`,
        `arn:aws:bedrock:us-east-2:${accountId}:data-automation-profile/us.data-automation-v1`,
        `arn:aws:bedrock:us-east-1:${accountId}:data-automation-profile/us.data-automation-v1`,
        bdaProject.attrProjectArn,
      ]
    }));

    // Add additional permissions for getting data automation status
    bdaProcessorLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        "bedrock:GetDataAutomationStatus"
      ],
      resources: [
        `arn:aws:bedrock:${region}:${accountId}:data-automation-invocation/*`
      ]
    }));

    // Create an IAM role for the Step Function
    const stepFunctionRole = new iam.Role(this, "BdaStepFunctionRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
      description: "Role for BDA Step Function execution"
    });

    // Grant the Step Function permission to invoke Lambda functions
    stepFunctionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["lambda:InvokeFunction"],
        resources: [
          bdaProcessorLambda.functionArn,
          bdaClassifierLambda.functionArn
        ]
      })
    );

    // Define the Step Function tasks
    const processorTask = new tasks.LambdaInvoke(this, 'BdaDocumentProcessorTask', {
      lambdaFunction: bdaProcessorLambda,
      outputPath: '$.Payload',
    });

    // Add retry configuration for processor task
    processorTask.addRetry({
      errors: [
        'Lambda.ServiceException',
        'Lambda.AWSLambdaException',
        'Lambda.SdkClientException',
        'Lambda.TooManyRequestsException'
      ],
      interval: cdk.Duration.seconds(1),
      maxAttempts: 3,
      backoffRate: 2,
    });

    // Define the second task - bda classifier
    const classifierTask = new tasks.LambdaInvoke(this, 'BdaClassifierTask', {
      lambdaFunction: bdaClassifierLambda,
      outputPath: '$.Payload',
      // Pass the outputs from processor task to classifier task
      payload: sfn.TaskInput.fromObject({
        "s3_uri.$": "$.s3_uri",
        "bda_invocation_arn.$": "$.bda_invocation_arn"
      }),
    });

    // Add retry configuration for classifier task
    classifierTask.addRetry({
      errors: [
        'Lambda.ServiceException',
        'Lambda.AWSLambdaException',
        'Lambda.SdkClientException',
        'Lambda.TooManyRequestsException'
      ],
      interval: cdk.Duration.seconds(1),
      maxAttempts: 3,
      backoffRate: 2,
    });

    // Chain the tasks to create the workflow
    const definition = processorTask.next(classifierTask);

    // Create the Step Function state machine
    const bdaStepFunction = new sfn.StateMachine(this, 'BdaStepFunction', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      role: stepFunctionRole,
      timeout: cdk.Duration.minutes(30),
    });

    // Create an EventBridge rule to capture S3 events and invoke the Step Function
    const s3ObjectCreatedRule = new events.Rule(this, 'S3ObjectCreatedRule', {
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['Object Created'],
        detail: {
          bucket: {
            name: [bdaInputBucket.bucketName]
          },
          object: {
            key: [{ suffix: '.pdf' }]
          }
        }
      },
      description: 'Rule to trigger Step Function when PDF is uploaded to S3'
    });

    // Add the Step Function as a target for the EventBridge rule
    s3ObjectCreatedRule.addTarget(new targets.SfnStateMachine(bdaStepFunction));

    // Output information on the resources
    new cdk.CfnOutput(this, "BdaInputBucketName", {
      value: bdaInputBucket.bucketName,
    });

    new cdk.CfnOutput(this, "BdaOutputBucketName", {
      value: bdaOutputBucket.bucketName,
    });

    // Add outputs for blueprint ARNs
    new cdk.CfnOutput(this, "DriversLicenseBlueprintArn", {
      value: dlBlueprint.attrBlueprintArn,
    });

    new cdk.CfnOutput(this, "HealthInsuranceClaimBlueprintArn", {
      value: healthInsuranceClaimBlueprint.attrBlueprintArn,
    });

    new cdk.CfnOutput(this, "MedicalInsuranceCardsBlueprintArn", {
      value: medicalInsuranceCardsBlueprint.attrBlueprintArn,
    });

    new cdk.CfnOutput(this, "SurgicalPathologyReportBlueprintArn", {
      value: surgicalPathologyReportBlueprint.attrBlueprintArn,
    });

    new cdk.CfnOutput(this, "BankStatementBlueprintArn", {
      value: bankStatementBlueprint.attrBlueprintArn,
    });

    // Output user pool information if we created new resources
    if (!useExistingResources) {
      new cdk.CfnOutput(this, "UserPoolId", {
        value: userPoolId,
      });

      new cdk.CfnOutput(this, "UserPoolClientId", {
        value: userPoolClientId,
      });
    }

    new cdk.CfnOutput(this, "BdaStepFunctionArn", {
      value: bdaStepFunction.stateMachineArn,
      description: "ARN of the BDA processing Step Function"
    });
  }
}